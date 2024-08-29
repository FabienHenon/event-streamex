defmodule EventStreamex.Events do
  alias EventStreamex.EventsProtocol

  require Logger

  def schema_module(module), do: module |> Module.split() |> Enum.drop(-1) |> Module.concat()

  defmacro __using__(opts) do
    module_name = String.to_atom(Keyword.get(opts, :module_name, "Events"))
    table_name = String.to_atom(Keyword.get(opts, :schema, "events"))
    table_name_str = Keyword.get(opts, :schema, nil)
    app_name = Keyword.get(opts, :name, nil)
    extra_channels = Keyword.get(opts, :extra_channels, [])
    application = Keyword.get(opts, :application, Application)
    scheduler = Keyword.get(opts, :scheduler, EventStreamex.Operators.Scheduler)

    if(is_nil(table_name_str), do: raise("schema attribute not set in EventStreamex.Events"))
    if(is_nil(app_name), do: raise("name attribute not set in EventStreamex.Events"))

    quote do
      Module.register_attribute(
        __MODULE__,
        :table_name,
        persist: true
      )

      Module.register_attribute(
        __MODULE__,
        :module_name,
        persist: true
      )

      defimpl EventStreamex.EventsProtocol do
        def table_name(_) do
          :attributes
          |> @for.__info__()
          |> Keyword.get(:table_name, [])
          |> List.first()
        end

        def module(_) do
          :attributes
          |> @for.__info__()
          |> Keyword.get(:module_name, [])
          |> List.first()
        end
      end

      @module_name Module.concat([__MODULE__, unquote(module_name)])
      @table_name unquote(table_name_str)

      defmodule Module.concat([__MODULE__, unquote(module_name)]) do
        @schema_module EventStreamex.Events.schema_module(__MODULE__)

        use WalEx.Event, name: unquote(app_name)

        require Logger

        on_insert(
          unquote(table_name),
          %{},
          [],
          fn items ->
            Logger.debug("#{unquote(table_name)} inserted: #{inspect(items)}")

            items
            |> Enum.each(&EventStreamex.Events.process_event(unquote(scheduler), &1))

            EventStreamex.Events.handle_events_broadcast(
              items,
              :on_insert,
              @schema_module,
              unquote(table_name_str),
              unquote(extra_channels),
              unquote(application)
            )

            :ok
          end
        )

        on_update(
          unquote(table_name),
          %{},
          [],
          fn items ->
            Logger.debug("#{unquote(table_name)} updated: #{inspect(items)}")

            items
            |> Enum.each(&EventStreamex.Events.process_event(unquote(scheduler), &1))

            EventStreamex.Events.handle_events_broadcast(
              items,
              :on_update,
              @schema_module,
              unquote(table_name_str),
              unquote(extra_channels),
              unquote(application)
            )

            :ok
          end
        )

        on_delete(
          unquote(table_name),
          %{},
          [],
          fn items ->
            Logger.debug("#{unquote(table_name)} deleted: #{inspect(items)}")

            items
            |> Enum.each(&EventStreamex.Events.process_event(unquote(scheduler), &1))

            EventStreamex.Events.handle_events_broadcast(
              items,
              :on_delete,
              @schema_module,
              unquote(table_name_str),
              unquote(extra_channels),
              unquote(application)
            )

            :ok
          end
        )
      end
    end
  end

  @doc false
  def process_event(scheduler, event) do
    start_metadata = %{type: :enqueue_event, event: event}

    :telemetry.span(
      [:event_streamex, :enqueue_event],
      start_metadata,
      fn ->
        result = EventStreamex.Operators.Scheduler.process_event(scheduler, event)
        {result, start_metadata}
      end
    )
  end

  @doc false
  # Sends the received items to the listeners
  def handle_events_broadcast(
        items,
        event_name,
        schema_module,
        table_name,
        extra_channels,
        application
      ) do
    [adapter: pubsub_adapter, name: pubsub] = application.get_env(:event_streamex, :pubsub)

    items
    |> Enum.map(fn item ->
      event = %{
        item
        | new_record: item.new_record && struct(schema_module, item.new_record),
          old_record: item.old_record && struct(schema_module, item.old_record)
      }

      used_record = if(event_name == :on_delete, do: event.old_record, else: event.new_record)

      Logger.debug("#{table_name} broacasted in channel: #{table_name}")

      # General channel for the resource
      pubsub_adapter.broadcast(
        pubsub,
        "#{table_name}",
        {event_name, [], event}
      )

      Logger.debug("#{table_name} broacasted in channel: #{table_name}/#{used_record.id}")

      # Channel for direct access to the resource
      pubsub_adapter.broadcast(
        pubsub,
        "#{table_name}/#{used_record.id}",
        {event_name, :direct, event}
      )

      # Other scoped channels
      extra_channels
      |> Enum.each(fn %{scopes: [_ | _] = scopes} ->
        Logger.debug(
          "#{table_name} broadcasted in channel: #{scopes |> Enum.map(&"#{elem(&1, 1)}/#{Map.get(used_record, elem(&1, 0))}") |> Enum.join("/")}/#{table_name}"
        )

        # Scoped channels are in the form : "parent_entities/123/entities"
        pubsub_adapter.broadcast(
          pubsub,
          "#{scopes |> Enum.map(&"#{elem(&1, 1)}/#{Map.get(used_record, elem(&1, 0))}") |> Enum.join("/")}/#{table_name}",
          {event_name,
           scopes
           |> Enum.map(&{elem(&1, 1), Map.get(used_record, elem(&1, 0))}), event}
        )
      end)
    end)
  end

  defimpl EventsProtocol, for: Atom do
    def table_name(module) do
      :attributes
      |> module.__info__()
      |> Keyword.get(:table_name, [])
      |> List.first()
    end

    def module(module) do
      :attributes
      |> module.__info__()
      |> Keyword.get(:module_name, [])
      |> List.first()
    end
  end

  @spec registered_event_listeners() :: [module()]
  def registered_event_listeners() do
    modules =
      case EventsProtocol.__protocol__(:impls) do
        {:consolidated, modules} ->
          modules

        _ ->
          Protocol.extract_impls(
            EventsProtocol,
            :code.lib_dir()
          )
      end

    for module <- modules,
        module.__info__(:attributes)
        |> Keyword.has_key?(:table_name),
        do: module
  end

  @spec get_modules_and_subscriptions() :: {[module()], [binary()]}
  def get_modules_and_subscriptions() do
    registered_event_listeners()
    |> Enum.map(&{&1 |> EventsProtocol.module(), &1 |> EventsProtocol.table_name()})
    |> Enum.unzip()
  end
end
