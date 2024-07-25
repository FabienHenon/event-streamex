defmodule EventStreamex.EventListener do
  require Logger

  defmacro __using__(opts) do
    table_name = Keyword.get(opts, :schema, "events")
    subscriptions = Keyword.get(opts, :subscriptions, [:direct, :unscoped])
    application = Keyword.get(opts, :application, Application)

    source_modules = __CALLER__.context_modules |> Enum.map(&Atom.to_string/1) |> Enum.join("/")

    quote do
      require Logger
      import EventStreamex.EventListener

      def mount(_params, _session, socket) do
        {:ok,
         handle_subscriptions(
           fn channel ->
             [adapter: pubsub_adapter, name: pubsub] =
               unquote(application).get_env(:event_streamex, :pubsub)

             pubsub_adapter.subscribe(pubsub, channel)
           end,
           :subscribed,
           socket,
           unquote(table_name),
           unquote(subscriptions),
           unquote(source_modules),
           %{},
           &Phoenix.LiveView.put_private/3
         )}
      end

      def terminate(_reason, socket) do
        handle_subscriptions(
          fn channel ->
            [adapter: pubsub_adapter, name: pubsub] =
              unquote(application).get_env(:event_streamex, :pubsub)

            pubsub_adapter.unsubscribe(pubsub, channel)
          end,
          :unsubscribed,
          socket,
          unquote(table_name),
          unquote(subscriptions),
          unquote(source_modules),
          Map.get(socket.private, :event_params, %{}),
          &Phoenix.LiveView.put_private/3
        )
      end

      def handle_params(params, _url, socket) do
        {:noreply,
         handle_subscriptions(
           fn channel ->
             [adapter: pubsub_adapter, name: pubsub] =
               unquote(application).get_env(:event_streamex, :pubsub)

             pubsub_adapter.subscribe(pubsub, channel)
           end,
           :subscribed,
           socket,
           unquote(table_name),
           unquote(subscriptions),
           unquote(source_modules),
           params,
           &Phoenix.LiveView.put_private/3
         )}
      end

      def handle_info(
            {:on_insert, _scope, _item},
            socket
          ),
          do: {:noreply, socket}

      def handle_info(
            {:on_update, _scope, _item},
            socket
          ),
          do: {:noreply, socket}

      def handle_info(
            {:on_delete, _scope, _item},
            socket
          ),
          do: {:noreply, socket}

      defoverridable mount: 3, terminate: 2, handle_params: 3, handle_info: 2

      @doc """
      Manually handles subscriptions, if the parameters used for scoped channels
      are handled in a different way than just getting a query parameter.

      ## Params

      * `socket`: The current socket
      * `params`: A map containing the parameters to use for scoped channels. Each scope field must be present in the params map as a string

      ## Returns

      The updated socket

      ## Example

      ```elixir
      defmodule MyModule do
        use EventStreamex.EventListener,
          schema: "comments",
          subscriptions: [%{scopes: [post_id: "posts"]}]

        def handle_params(%{"id" => id}, _url, socket) do
          entity = MyEntity.get(id)

          {:noreply,
            handle_subscriptions(
              socket,
              %{"post_id" => entity.post_id}
            )}
        end
      end
      ```

      In this example, I need a scoped channel with a `post_id` field.
      This field is not present in the url (which is a bad practice), but
      I know I can find it in the entity I fetch.

      Here, I don't have to call the `super()` function because I do not need
      the `EventListener` to handle the subscriptions as I do it manually.
      """
      def handle_subscriptions(socket, params) do
        handle_subscriptions(
          fn channel ->
            [adapter: pubsub_adapter, name: pubsub] =
              unquote(application).get_env(:event_streamex, :pubsub)

            pubsub_adapter.subscribe(pubsub, channel)
          end,
          :subscribed,
          socket,
          unquote(table_name),
          unquote(subscriptions),
          unquote(source_modules),
          params,
          &Phoenix.LiveView.put_private/3
        )
      end
    end
  end

  @doc false
  def handle_subscriptions(
        subscriber,
        type,
        socket,
        table_name,
        subscriptions,
        source_modules,
        params,
        put_private
      ) do
    Enum.reduce(subscriptions, socket, fn sub, curr_socket ->
      handle_subscription(
        subscriber,
        type,
        curr_socket,
        table_name,
        sub,
        source_modules,
        params,
        put_private
      )
    end)
  end

  @doc false
  defp handle_subscription(
         subscriber,
         type,
         socket,
         table_name,
         :unscoped,
         source_modules,
         _params,
         put_private
       ) do
    subscription_state = Map.get(socket.private, :subscribed?, default_subscribed_state())

    if check_subscribed_status(subscription_state.unscoped, type) do
      Logger.debug("#{source_modules} #{inspect(type)} channel: #{table_name}")

      # General channel for the resource
      subscriber.("#{table_name}")

      socket
      |> put_private.(
        :subscribed?,
        set_unscoped_subscription_state(subscription_state, type)
      )
    else
      socket
    end
  end

  @doc false
  defp handle_subscription(
         subscriber,
         type,
         socket,
         table_name,
         :direct,
         source_modules,
         %{"id" => id} = params,
         put_private
       ) do
    subscription_state = Map.get(socket.private, :subscribed?, default_subscribed_state())

    if check_subscribed_status(subscription_state.direct, type) do
      Logger.debug("#{source_modules} #{inspect(type)} channel: #{table_name}/#{id}")

      # Channel for direct access to the resource
      subscriber.("#{table_name}/#{id}")

      socket
      |> put_private.(
        :subscribed?,
        set_direct_subscription_state(subscription_state, type)
      )
      |> put_private.(
        :event_params,
        update_event_params(Map.get(socket.private, :event_params, %{}), params, ["id"])
      )
    else
      socket
    end
  end

  @doc false
  defp handle_subscription(
         _subscriber,
         _type,
         socket,
         _table_name,
         :direct,
         _source_modules,
         _params,
         _put_private
       ) do
    socket
  end

  @doc false
  defp handle_subscription(
         subscriber,
         type,
         socket,
         table_name,
         %{scopes: scopes},
         source_modules,
         params,
         put_private
       ) do
    subscription_state = Map.get(socket.private, :subscribed?, default_subscribed_state())

    if check_subscribed_status(
         is_subscribed_to_scope?(subscription_state, %{scopes: scopes}),
         type
       ) do
      # Scoped channel
      serialized_scopes =
        scopes
        |> Enum.map(fn {field, entity} ->
          field_value = Map.get(params, Atom.to_string(field), nil)

          field_value && "#{entity}/#{field_value}"
        end)

      if not Enum.any?(serialized_scopes, &is_nil(&1)) do
        path =
          "#{serialized_scopes |> Enum.join("/")}/#{table_name}"

        Logger.debug("#{source_modules} #{inspect(type)} channel: #{path}")

        # Scoped channels are in the form : "parent_entities/123/entities"
        subscriber.(path)

        socket
        |> put_private.(
          :subscribed?,
          set_scoped_subscription_state(subscription_state, %{scopes: scopes}, type)
        )
        |> put_private.(
          :event_params,
          update_event_params(
            Map.get(socket.private, :event_params, %{}),
            params,
            scopes |> Enum.map(&Atom.to_string(elem(&1, 0)))
          )
        )
      else
        socket
      end
    else
      socket
    end
  end

  @doc false
  def default_subscribed_state(), do: %{direct: false, unscoped: false}

  @doc false
  def set_direct_subscription_state(subscription_state, :subscribed),
    do: %{subscription_state | direct: true}

  def set_direct_subscription_state(subscription_state, :unsubscribed),
    do: %{subscription_state | direct: false}

  @doc false
  def set_unscoped_subscription_state(subscription_state, :subscribed),
    do: %{subscription_state | unscoped: true}

  def set_unscoped_subscription_state(subscription_state, :unsubscribed),
    do: %{subscription_state | unscoped: false}

  @doc false
  def set_scoped_subscription_state(subscription_state, %{scopes: scopes}, :subscribed),
    do: Map.put(subscription_state, serialize_scopes(scopes), true)

  def set_scoped_subscription_state(subscription_state, %{scopes: scopes}, :unsubscribed),
    do: Map.put(subscription_state, serialize_scopes(scopes), false)

  @doc false
  def is_subscribed_to_scope?(subscription_state, %{scopes: scopes}) do
    Map.get(subscription_state, serialize_scopes(scopes), false)
  end

  @doc false
  defp serialize_scopes(scopes) do
    scopes
    |> Enum.map(&"#{Atom.to_string(elem(&1, 0))}:#{elem(&1, 1)}")
    |> Enum.join("/")
  end

  @doc false
  def update_event_params(event_params, params, keys) do
    Enum.reduce(keys, event_params, &Map.put(&2, &1, Map.get(params, &1, nil)))
  end

  defp check_subscribed_status(subscribed?, :subscribed), do: not subscribed?
  defp check_subscribed_status(subscribed?, :unsubscribed), do: subscribed?
end
