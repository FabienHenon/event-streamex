defmodule EventStreamex.Operators.Operator do
  alias EventStreamex.Operators.OperatorsProtocol

  require Logger

  defmacro __using__(opts) do
    repo = Keyword.get(opts, :repo, nil)
    sql_adapter = Keyword.get(opts, :sql_adapter, Ecto.Adapters.SQL)
    id_field = Keyword.get(opts, :id, :id)
    schema = Keyword.get(opts, :schema, nil)

    if(is_nil(schema), do: raise("schema attribute not set in EventStreamex.Operators.Operator"))
    if(is_nil(repo), do: raise("repo attribute not set in EventStreamex.Operators.Operator"))

    quote do
      import EventStreamex.Operators.Operator

      Module.register_attribute(
        __MODULE__,
        :schemas,
        persist: true
      )

      Module.register_attribute(
        __MODULE__,
        :event_types,
        persist: true
      )

      defimpl EventStreamex.Operators.OperatorsProtocol do
        def schemas(_) do
          :attributes
          |> @for.__info__()
          |> Keyword.get(:schemas, [])
        end

        def event_types(_) do
          :attributes
          |> @for.__info__()
          |> Keyword.get(:event_types, [])
        end
      end

      use GenServer
      require Logger

      @schemas []
      @event_types []
      @repo unquote(repo)
      @sql_adapter unquote(sql_adapter)
      @id_field unquote(id_field)
      @schema unquote(schema)
      @id_type :uuid

      def start(arg) do
        GenServer.start(__MODULE__, arg, name: __MODULE__)
      end

      @impl true
      def init(event) do
        {:ok, event}
      end

      @impl true
      def handle_info(
            :process,
            event
          ) do
        event_mapper = %{update: :on_update, insert: :on_insert, delete: :on_delete}

        case process_event(event, Map.get(event_mapper, event.type, nil), event.source.table) do
          {:ok, res} ->
            Logger.debug(
              "Operator #{inspect(__MODULE__)} terminated successfully with result #{inspect(res)}"
            )

            {:stop, :normal, event}

          reason ->
            Logger.warning(
              "Operator #{inspect(__MODULE__)} terminated with error #{inspect(reason)}"
            )

            {:stop, reason, event}
        end
      end
    end
  end

  defimpl OperatorsProtocol, for: Atom do
    def schemas(module) do
      :attributes
      |> module.__info__()
      |> Keyword.get(:schemas, [])
    end

    def event_types(module) do
      :attributes
      |> module.__info__()
      |> Keyword.get(:event_types, [])
    end
  end

  defmacro on_event(events, entities, callback) do
    events
    |> Macro.expand_once(__CALLER__)
    |> Enum.map(fn event ->
      entities
      |> Macro.expand_once(__CALLER__)
      |> Enum.map(fn entity ->
        quote do
          Module.put_attribute(
            __MODULE__,
            :event_types,
            [
              unquote(event)
              | Module.get_attribute(__MODULE__, :event_types, [])
            ]
            |> Enum.uniq()
          )

          Module.put_attribute(
            __MODULE__,
            :schemas,
            [
              unquote(entity)
              | Module.get_attribute(__MODULE__, :schemas, [])
            ]
            |> Enum.uniq()
          )

          def process_event(curr_event, unquote(event), unquote(entity)) do
            unquote(callback).(curr_event)
          end
        end
      end)
    end)
    |> Enum.concat([
      quote do
        def process_event(_curr_event, event, entity) do
          Logger.debug(
            "Operator #{inspect(__MODULE__)} ignored event type #{inspect(event)} for entity #{inspect(entity)}"
          )

          {:ok, :no_op}
        end
      end
    ])
  end

  defmacro all_events(), do: quote(do: [:on_insert, :on_update, :on_delete])
  defmacro events(events), do: quote(do: unquote(events))
  defmacro event(event), do: quote(do: [unquote(event)])

  defmacro entities(entities), do: quote(do: unquote(entities))
  defmacro entity(entity), do: quote(do: [unquote(entity)])

  defmacro query(query, ids_mapping, delete_with) do
    quote do
      fn event ->
        res =
          if event.type == :delete && Enum.member?(unquote(delete_with), event.source.table) do
            @sql_adapter.query(
              @repo,
              "DELETE FROM #{@schema} WHERE #{to_string(@id_field)} = $1",
              [
                get_id_from_event(event, unquote(ids_mapping), @id_type)
              ]
            )
          else
            @sql_adapter.query(@repo, unquote(query), [
              get_id_from_event(event, unquote(ids_mapping), @id_type)
            ])
          end

        Logger.debug("Operator #{inspect(__MODULE__)} query executed with result #{inspect(res)}")

        res
      end
    end
  end

  defmacro id(mapping) when is_list(mapping), do: quote(do: unquote(mapping))

  defmacro delete_with(schemas) when is_list(schemas), do: quote(do: unquote(schemas))
  defmacro no_delete(), do: quote(do: [])

  def get_id_from_event(event, ids_mapping, id_type) do
    {_table, field} =
      ids_mapping
      |> Enum.find({"default", :id}, fn {schema, _field} -> event.source.table == schema end)

    record = if(event.type == :delete, do: event.old_record, else: event.new_record)

    Map.get(record, field, nil)
    |> cast_id(id_type)
  end

  defp cast_id(id, :uuid), do: UUID.string_to_binary!(id)
  defp cast_id(id, _type), do: id

  def registered_operators() do
    modules =
      case OperatorsProtocol.__protocol__(:impls) do
        {:consolidated, modules} ->
          modules

        _ ->
          Protocol.extract_impls(
            OperatorsProtocol,
            :code.lib_dir()
          )
      end

    for module <- modules,
        module.__info__(:attributes)
        |> Keyword.has_key?(:schemas),
        do: module
  end

  def get_modules_for_event(event_type, schema) do
    registered_operators()
    |> Enum.filter(&(&1 |> OperatorsProtocol.schemas() |> Enum.member?(schema)))
    |> Enum.filter(&(&1 |> OperatorsProtocol.event_types() |> Enum.member?(event_type)))
  end
end
