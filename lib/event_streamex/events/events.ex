defmodule EventStreamex.Events do
  @moduledoc """
  This module can be used in schema modules and is necessary to mark an
  entity for event listening.

  This is what connects an entity to the WAL events.

  If you don't use this module for an entity you won't be able to receive
  its events, which will prevent you from listening to events in live views (`EventStreamex.EventListener`),
  and to use them in operators to update other entities (`EventStreamex.Operators.Operator`).

  To mark an entity for event listening, you only have to `use` this module in
  the schema module of the entity:

  ```elixir
  defmodule MyApp.Blog.Comment do
    use MyApp.Schema

    use EventStreamex.Events,
      name: :my_app,
      schema: "comments",
      extra_channels: [%{scopes: [post_id: "posts"]}]

    import Ecto.Changeset

    schema "comments" do
      field :message, :string
      field :post_id, Ecto.UUID

      timestamps(type: :utc_datetime)
    end

    @doc false
    def changeset(comment, attrs) do
      comment
      |> cast(attrs, [:id, :message, :post_id])
      |> validate_required([:message, :post_id])
    end
  end
  ```

  In this example, we have a `comments` entity that we want to mark for event listening.

  This `comments` entity is scoped by a `post_id` related to a `posts` entity.
  So we needed to add an `extra_channels` so that comment events will also be emitted in a channel
  specific for his scopes (See `EventStreamex.EventListener` for more details on scopes)

  Several scopes can be used for an entity. The order used for scopes must be the same
  when you define it in `EventStreamex.EventListener`.

  Here is an example with another scope:

  ```elixir
  use EventStreamex.Events,
      name: :my_app,
      schema: "comments",
      extra_channels: [%{scopes: [org_id: "organizations", post_id: "posts"]}]
  ```

  Here we say a comment has 2 scopes: an organization and a post.
  That means the entity must have the 2 fields: `org_id` and `post_id`.

  ## How it works

  As said earlier, this module marks the entity for WAL events.
  Which allows you to listen to its changes and to use it in operators.

  Under the hoods, this module will create a sub module inside the module you use it in.
  And it will define functions that will be called each time something happens with this entity (insert, update, delete).

  Then, it will dispatch the event to all operators listening to this entity (`EventStreamex.Operators.Operator`).
  And it will emit an event in the configured pubsub module in several channels:

  * `direct`: We emit in a specific channel containing the ID of the enity
  * `unscoped`: We emit in a channel for changes of all entities of this type
  * `scopes`: We emit in a channel for entities matching the given scopes (Like in the example above)

  *For more information about scopes and channels, see `EventStreamex.EventListener`.*

  ## `use` params

  When you `use` this module, you will be able to specify these parameters:

  * `:module_name`: The name of the module that will be created in your module to perform the events logic (Defaults to `"Events"`)
  * `:schema`: The name of the entity as a string (mandatory field)
  * `:name`: The name of your application (Mandatory field)
  * `:extra_channels`: The channels to emit events to. `:direct` and `:unscoped` are always used, and you can add your own scopes for this entity (Defaults to `[]`)
  * `:application`: The application module to use to retrieve config values (Defaults to `Application`)
  * `:scheduler`: If you want to use another scheduler to handle events processed by operators (Defaults to `EventStreamex.Operators.Scheduler`)

  """

  @moduledoc since: "1.0.0"
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

      @table_name unquote(table_name_str)
      @module_name Module.concat([__MODULE__, unquote(module_name)])

      defmodule Module.concat([__MODULE__, unquote(module_name)]) do
        @schema_module EventStreamex.Events.schema_module(__MODULE__)

        use WalEx.Event, name: unquote(app_name)

        require Logger

        def on_insert_broadcast(item) do
          EventStreamex.Events.handle_event_broadcast(
            item,
            :on_insert,
            @schema_module,
            unquote(table_name_str),
            unquote(extra_channels),
            unquote(application)
          )
        end

        on_insert(
          unquote(table_name),
          %{},
          [{Module.concat([@schema_module, unquote(module_name)]), :on_insert_broadcast}],
          fn items ->
            Logger.debug("#{unquote(table_name)} inserted: #{inspect(items)}")

            items
            |> Enum.each(&EventStreamex.Events.process_event(unquote(scheduler), &1))

            :ok
          end
        )

        def on_update_broadcast(item) do
          EventStreamex.Events.handle_event_broadcast(
            item,
            :on_update,
            @schema_module,
            unquote(table_name_str),
            unquote(extra_channels),
            unquote(application)
          )
        end

        on_update(
          unquote(table_name),
          %{},
          [{Module.concat([@schema_module, unquote(module_name)]), :on_update_broadcast}],
          fn items ->
            Logger.debug("#{unquote(table_name)} updated: #{inspect(items)}")

            items
            |> Enum.each(&EventStreamex.Events.process_event(unquote(scheduler), &1))

            :ok
          end
        )

        def on_delete_broadcast(item) do
          EventStreamex.Events.handle_event_broadcast(
            item,
            :on_delete,
            @schema_module,
            unquote(table_name_str),
            unquote(extra_channels),
            unquote(application)
          )
        end

        on_delete(
          unquote(table_name),
          %{},
          [{Module.concat([@schema_module, unquote(module_name)]), :on_delete_broadcast}],
          fn items ->
            Logger.debug("#{unquote(table_name)} deleted: #{inspect(items)}")

            items
            |> Enum.each(&EventStreamex.Events.process_event(unquote(scheduler), &1))

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
  # Sends the received item to the listeners
  def handle_event_broadcast(
        item,
        event_name,
        schema_module,
        table_name,
        extra_channels,
        application
      ) do
    [adapter: pubsub_adapter, name: pubsub] = application.get_env(:event_streamex, :pubsub)

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
      {event_name, [], table_name, event}
    )

    Logger.debug("#{table_name} broacasted in channel: #{table_name}/#{used_record.id}")

    # Channel for direct access to the resource
    pubsub_adapter.broadcast(
      pubsub,
      "#{table_name}/#{used_record.id}",
      {event_name, :direct, table_name, event}
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
         |> Enum.map(&{elem(&1, 1), Map.get(used_record, elem(&1, 0))}), table_name, event}
      )
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

  @doc false
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

  @doc false
  def get_modules_and_subscriptions() do
    registered_event_listeners()
    |> Enum.map(&{&1 |> EventsProtocol.module(), &1 |> EventsProtocol.table_name()})
    |> Enum.unzip()
  end
end
