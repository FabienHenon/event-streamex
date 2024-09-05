defmodule EventStreamex.EventListener do
  @moduledoc """
  Listens for new database WAL events in a `Phoenix.LiveView`.

  As soon as something happens in the database (insert, delete, update),
  a WAL event is sent and dispatched to operators that listen to it, and to
  live views that uses the `EventStreamex.EventListener` module.

  Let's say you have an entity named `comments` and you have a live view that
  shows a list of thoses comments.

  For a better user experience you would like to synchronize this list in realtime
  as soon as a new comment is created.

  For this matter you will use the `EventStreamex.EventListener` module in your live view:

  ```elixir
  defmodule MyApp.CommentLive.Index do
    use MyApp, :live_view

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [%{scopes: [post_id: "posts"]}]

    alias MyApp.Blog
    alias MyApp.Blog.Comment

    @impl true
    def mount(params, session, socket) do
      super(params, session, socket)
    end

    @impl true
    def handle_params(%{"post_id" => post_id} = params, url, socket) do
      {_res, socket} = super(params, url, socket)

      {:noreply,
      socket
      |> stream(:comments, Blog.list_comments(post_id))
      |> assign(:post_id, post_id)
    end

    @impl true
    def handle_info(
          {:on_insert, [{"posts", post_id}], "comments", comment},
          %{assigns: %{post_id: post_id}} = socket
        ) do
      {:noreply,
      socket
      |> stream_insert(:comments, comment.new_record)
    end

    @impl true
    def handle_info(
          {:on_update, [{"posts", post_id}], "comments", comment},
          %{assigns: %{post_id: post_id}} = socket
        ) do
      {:noreply,
      socket
      |> stream_insert(:comments, comment.new_record)
    end

    @impl true
    def handle_info(
          {:on_delete, [{"posts", post_id}], "comments", comment},
          %{assigns: %{post_id: post_id}} = socket
        ) do
      {:noreply,
      socket
      |> stream_delete(:comments, comment.old_record)
    end
  end
  ```

  This code will update the comments list as soon as a new comment is either
  inserted, deleted or udpated (notice that here, comments are linked to a `post` via the `post_id` field and we load only the comments for a specific post).

  **For this code to work you will also have to use the `EventStreamex.Events` module to mark the entity for WAL listening. Without doing so, the `comments` entity will not be listened from WAL events, and thus, will not be dispatched to operators and live views**

  ## How it works

  `use EventStreamex.EventListener` will do the "magic" by subscribing to the entity
  changes in `Phoenix.LiveView.mount/3`, `Phoenix.LiveView.terminate/3` and `Phoenix.LiveView.handle_params/3`
  callbacks.

  That means that is you override these callbacks you have to call the `super` function
  so that the "magic" is done.

  The "magic" in question is a subscription to several channels in a pubsub module.
  There are 3 kinds a channels this module will automatically subscribe to:
  * `direct`: We subscribe to a specific entity changes (by it's ID)
  * `unscoped`: We subscribes to changes of all entities in a table
  * `scopes`: We subscribe to changes in entities that match a specific scope (like having a specific `post_id` in the example above)

  We can use the 3 kinds a subscriptions at the same time:

  ```elixir
  use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [:direct, :unscoped, %{scopes: [post_id: "posts"]}]
  ```

  By default, we automatically subscribe to the `direct` and `unscoped` channels.

  All events are received in `Phoenix.LiveView.handle_info/2` callbacks with a message of this form:

  ```elixir
  {:on_insert | :on_delete | :on_update, :direct | [] | [{binary(), id()}], binary(), entity_change()}
  ```

  *More information about each kind of message in the subsections below*

  ### `:direct`

  The `:direct` channel subscribes to a specific entity by its `id` field.
  This is most usefull in `show` or `edit` views where you only need to show one specific entity.

  ```elixir
  use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [:direct]
  ```

  That means that if you want to subscription to be effective, you will have to receive the id of entity
  in the params.
  Or, pass it manually when you call the `super` function:

  ```elixir
  @impl true
  def mount(_params, session, socket) do
    super(%{"id" => get_resource_id(session)}, session, socket)
  end
  ```

  The changes in the entity will be received in the `Phoenix.LiveView.handle_info/2` callback
  with a message of this form:

  ```elixir
  {:on_insert | :on_update | :on_delete, :direct, binary(), entity_change()}
  ```

  Here is an example:

  ```elixir
  defmodule MyApp.CommentLive.Show do
    use MyApp, :live_view

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [:direct]

    alias MyApp.Blog

    @impl true
    def mount(params, session, socket) do
      super(params, session, socket)
    end

    @impl true
    def handle_params(%{"id" => id} = params, url, socket) do
      {_res, socket} = super(params, url, socket)

      {:noreply,
      socket
      |> assign(:id, id)
      |> assign(:comment, Blog.get_comment!(id))}
    end

    @impl true
    def handle_info({:on_insert, :direct, "comments", _comment}, socket) do
      # Should never happen because the entity already exists
      {:noreply, socket}
    end

    @impl true
    def handle_info({:on_update, :direct, "comments", comment}, socket) do
      {:noreply,
      socket
      |> assign(:comment, comment.new_record)
    end

    @impl true
    def handle_info({:on_delete, :direct, "comments", _comment}, socket) do
      # Do some redirection stuff eventually
      {:noreply, socket |> put_flash(:warning, "This comment has been deleted")}
    end
  end
  ```

  ### `:unscoped`

  The `:unscoped` channel subscribes to changes of all entities.
  This is what you will use for `index` views when your entity is not scoped.

  ```elixir
  use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [:unscoped]
  ```

  The changes in the entities will be received in the `Phoenix.LiveView.handle_info/2` callback
  with a message of this form:

  ```elixir
  {:on_insert | :on_update | :on_delete, [], binary(), entity_change()}
  ```

  Here is an example:

  ```elixir
  defmodule MyApp.CommentLive.Index do
    use MyApp, :live_view

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [:unscoped]

    alias MyApp.Blog
    alias MyApp.Blog.Comment

    @impl true
    def mount(params, session, socket) do
      super(params, session, socket)
    end

    @impl true
    def handle_params(params, url, socket) do
      {_res, socket} = super(params, url, socket)

      {:noreply,
      socket
      |> stream(:comments, Blog.list_comments())
    end

    @impl true
    def handle_info(
          {:on_insert, [], "comments", comment}, socket
        ) do
      {:noreply,
      socket
      |> stream_insert(:comments, comment.new_record)
    end

    @impl true
    def handle_info(
          {:on_update, [], "comments", comment}, socket
        ) do
      {:noreply,
      socket
      |> stream_insert(:comments, comment.new_record)
    end

    @impl true
    def handle_info(
          {:on_delete, [],"comments",  comment}, socket
        ) do
      {:noreply,
      socket
      |> stream_delete(:comments, comment.old_record)
    end
  end
  ```

  ### `:scopes`

  The `:scopes` channel subscribes to changes of all entities that match some
  specific field values.

  To declare a scoped entity you will do like this:

  ```elixir
  use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [%{scopes: [post_id: "posts"]}]
  ```

  Here, `comments` have a `post_id` field related to a `posts` entity.

  You can also have several scopes for an entity (**The order matters for the matching in `Phoenix.LiveView.handle_info/2`**):

  ```elixir
  use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [%{scopes: [org_id: "organizations", post_id: "posts"]}]
  ```

  Here, `comments` have a `org_id` field related to a `organizations` entity, and a `post_id` field, related to a `posts` entity.

  As for `:direct` scopes, you will have to receive the scope fields in the params so that the module
  is able to subscribe to the correct channels.
  If your fields are not in the params or are named differently, you will have to pass them yourself to the `super` function:

  ```elixir
  @impl true
  def handle_params(%{"my_post_id" => my_post_id, "my_org_id" => my_org_id} = params, url, socket) do
    {_res, socket} = super(%{"post_id" => my_post_id, "org_id" => my_org_id}, url, socket)

    {:noreply,
    socket
    |> stream(:comments, Blog.list_comments(my_post_id, my_org_id))
  end
  ```

  Events will be received in the `c:Phoenix.LiveView.handle_info/2` callback, with messages of this form:

  ```elixir
  {:on_insert | :on_update | :on_delete, [{"related_scoped_entity", scope_id}], binary(), entity_change()}
  ```

  For instance, with our previous example, an insert event message will look like this:

  ```elixir
  {:on_insert, [{"organizations", org_id}, {"posts", post_id}], "comments", entity_change}
  ```

  **The order of scopes will be the same as the one you specified above in the `use`**.

  Here is a full example:


  ```elixir
  defmodule MyApp.CommentLive.Index do
    use MyApp, :live_view

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [%{scopes: [post_id: "posts"]}]

    alias MyApp.Blog
    alias MyApp.Blog.Comment

    @impl true
    def mount(params, session, socket) do
      super(params, session, socket)
    end

    @impl true
    def handle_params(%{"post_id" => post_id} = params, url, socket) do
      {_res, socket} = super(params, url, socket)

      {:noreply,
      socket
      |> stream(:comments, Blog.list_comments(post_id))
      |> assign(:post_id, post_id)
    end

    @impl true
    def handle_info(
          {:on_insert, [{"posts", post_id}], "comments", comment},
          %{assigns: %{post_id: post_id}} = socket
        ) do
      {:noreply,
      socket
      |> stream_insert(:comments, comment.new_record)
    end

    @impl true
    def handle_info(
          {:on_update, [{"posts", post_id}], "comments", comment},
          %{assigns: %{post_id: post_id}} = socket
        ) do
      {:noreply,
      socket
      |> stream_insert(:comments, comment.new_record)
    end

    @impl true
    def handle_info(
          {:on_delete, [{"posts", post_id}], "comments", comment},
          %{assigns: %{post_id: post_id}} = socket
        ) do
      {:noreply,
      socket
      |> stream_delete(:comments, comment.old_record)
    end
  end
  ```

  ## Entity change structure

  The event received in the `Phoenix.LiveView.handle_info/2` callbacks have information about
  the entity and its changes.

  Here is what the structure looks like:

  ```elixir
  %WalEx.Event{
    name: atom(),
    type: :insert | :update | :delete,
    source: %WalEx.Event.Source{
      name: String.t(),
      version: String.t(),
      db: String.t(),
      schema: String.t(),
      table: String.t(),
      columns: map()
    },
    new_record: map() | nil,
    old_record: map() | nil,
    changes: map() | nil,
    timestamp: DateTime.t(),
    lsn: {integer(), integer()}
  }
  ```

  *I am using the [WalEx package internally](https://github.com/cpursley/walex)*

  * `name`: The name of the entity (ie: `:comments` for a table named `comments`)
  * `type`: The type of event between `insert`, `update` and `delete`
  * `source`: Information about the event:
    * `name`: "WalEx"
    * `version`: Current version of `WalEx`
    * `db`: The name of the database
    * `schema`: Mostly `"public"`
    * `table`: The name of the table (ie: `"comments"`)
    * `columns`: A map of fields with their type (ie: `%{"id": "integer", "message": "varchar"}`)
  * `new_record`: The entity itself for `insert` and `update` events. `nil` for `delete` events.
  * `old_record`: The entity itself for `delete` events. `nil` for `insert` and `update` events.
  * `changes`: A map with the changes in the entity in `update` events, `nil` otherwise (see below)
  * `timestamp`: The timstamp of the event in `DateTime` type
  * `lsn`: A tuple containing information about the publication cursor

  ### `changes`

  When you receive an `update` event, you will also have the `changes` field set to a map containing
  the changes the entity received since the update.

  This map contains the changed fields as keys, and a map describing the change as value.
  This "change" map contains 2 fields:
  * `old_value`: The value before the update
  * `new_value`: The value after the update

  For instance, let's say you have a `comments` entity with 4 fields: `id`, `message`, `rating`, `timestamp`.

  You have a comment with these values:

  ```elixir
  %Comment{
    id: "dd4bc2ba-c7cc-4a05-a1c7-9f26cd9ab79f",
    message: "This is my first comment",
    rating: 4,
    timestamp: "2024-07-23T12:00:00Z"
  }
  ```

  Now, the comment is update this these new values:

  ```elixir
  %Comment{
    id: "dd4bc2ba-c7cc-4a05-a1c7-9f26cd9ab79f",
    message: "This is (not) my first comment (anymore)",
    rating: 5,
    timestamp: "2024-07-23T12:00:00Z"
  }
  ```

  The event structure will look like this:

  ```elixir
  %WalEx.Event{
    name: :comments,
    type: :update,
    source: %WalEx.Event.Source{
      name: "WalEx",
      version: "4.1.0",
      db: "postgresql",
      schema: "public",
      table: "comments",
      columns: %{
        id: "uuid",
        message: "varchar",
        rating: "integer",
        timestamp: "datetime"
      }
    },
    new_record: %Comment{
      id: "dd4bc2ba-c7cc-4a05-a1c7-9f26cd9ab79f",
      message: "This is (not) my first comment (anymore)",
      rating: 5,
      timestamp: "2024-07-23T12:00:00Z"
    },
    old_record: nil,
    changes: %{
      message: %{
        old_value: "This is my first comment",
        new_value: "This is (not) my first comment (anymore)"
      },
      rating: %{
        old_value: 4,
        new_value: 5
      }
    },
    timestamp: "2024-08-25T13:13:30Z",
    lsn: {0, 0}
  }
  ```

  ## Unsubscribing from events

  The unsubscribe from events is done automatically in the `c:Phoenix.LiveView.terminate/3` callback.
  You do not have anything to do except for calling the `super` function if you override this callback.

  ## Handling subscriptions later

  If you need, for any reason, to handle subscriptions at another moment than the `mount` and `handle_params` callbacks,
  we provide the `handle_subscriptions/2` function.

  This can be useful if the parameters used for scoped channels
  are handled in a different way than just getting them from query parameters.

  ### Params

  * `socket`: The current socket
  * `params`: A map containing the parameters to use for scoped channels. Each scope field must be present in the params map as a string

  ### Returns

  The updated socket

  ### Example

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

  You don't have to handle the unsubscribe either because it will be done for you in the
  `Phoenix.LiveView.terminate/3` callback.

  ## Subscribing to other entities' events

  Let's say you display posts and handle their creation.
  And when a new post is created you want to redirect to the detail page
  of this new post.
  But, the enitty used for the detail of this post is a derived entity from
  an operator.
  Thus, you can't be sure the entity will exist when you redirect to its detail page
  because of this asynchronicity of the event streaming architecture.

  So you would like to listen for this entity beeing created.
  Hopefully, you know its ID because you just created the post and this derived
  entity uses the same ID.

  So you can call the `subscribe_entiy/4` function to listen for the events coming from
  this derived entity.

  ```elixir
  subscribe_entity(socket, "post_with_comments_count", :direct, %{"id" => post_id})
  ```

  ### Params

  * `socket`: The socket
  * `entity_name`: The entity name to listen to as a string
  * `subscription`: The kind of channel you want to listen to (these are the same as for the module configuration: `:direct`, `:unscoped`, `%{scopes: []}`)
  * `params`: A map with the parameters needed for `:direct` and `:scopes` scopes.

  ### Return value

  The updated `socket`

  ### Received events

  Events are received the same way as other events but related to the entity:

  ```elixir
  @impl true
  def handle_info({:on_insert, :direct, "post_with_comments_count", post}, socket) do
    {:noreply, socket |> push_navigate(to: "/posts/\#{post.id}")}
  end
  ```

  ## Unsubscribing to other entities' events

  Use the function `unsubscribe_entity/3` to manually unsubscribe from an event's events.

  ```elixir
  unsubscribe_entity(socket, "post_with_comments_count", :direct)
  ```

  The params will be the same used for subscription so no need to pass them again.

  ### Params

  * `socket`: The socket
  * `entity_name`: The name of the entity to stop linstening
  * `subscription`: The kind of channel to stop listening to (these are the same as for the module configuration: `:direct`, `:unscoped`, `%{scopes: []}`)

  ### Return value

  The updated socket

  ## `use` params

  When you `use` this module, you will be able to specify these parameters:

  * `:schema`: The name of the entity as a string (mandatory field)
  * `:subscriptions`: The subscriptions you want to do (Defaults to `[:direct, :unscoped]`)
  * `:application`: The application module to use to retrieve config values (Defaults to `Application`)

  """

  @moduledoc since: "1.0.0"
  require Logger

  defmacro __using__(opts) do
    table_name = Keyword.get(opts, :schema, nil)
    subscriptions = Keyword.get(opts, :subscriptions, [:direct, :unscoped])
    application = Keyword.get(opts, :application, Application)

    if(is_nil(table_name), do: raise("schema attribute not set in EventStreamex.EventListener"))

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
        socket.private
        |> subscribed_entities()
        |> Enum.reduce(socket, fn entity, s ->
          {_subscription_state, event_params} =
            get_subscribe_state(s.private, entity)

          handle_subscriptions(
            fn channel ->
              [adapter: pubsub_adapter, name: pubsub] =
                unquote(application).get_env(:event_streamex, :pubsub)

              pubsub_adapter.unsubscribe(pubsub, channel)
            end,
            :unsubscribed,
            s,
            entity,
            get_entity_current_subscriptions(s.private, entity),
            unquote(source_modules),
            event_params,
            &Phoenix.LiveView.put_private/3
          )
        end)
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
            {:on_insert, _scope, _entity, _item},
            socket
          ),
          do: {:noreply, socket}

      def handle_info(
            {:on_update, _scope, _entity, _item},
            socket
          ),
          do: {:noreply, socket}

      def handle_info(
            {:on_delete, _scope, _entity, _item},
            socket
          ),
          do: {:noreply, socket}

      defoverridable mount: 3, terminate: 2, handle_params: 3, handle_info: 2

      @doc """
      Manually handles subscriptions, if the parameters used for scoped channels
      are handled in a different way than just getting them from query parameters.

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

      You don't have to handle the unsubscribe either because it will be done for you in the
      `c:Phoenix.LiveView.terminate/3` callback.
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

      @doc """
      Subscribes to another entity's events.

      Let's say you display posts and handle their creation.
      And when a new post is created you want to redirect to the detail page
      of this new post.
      But, the enitty used for the detail of this post is a derived entity from
      an operator.
      Thus, you can't be sure the entity will exist when you redirect to its detail page
      because of this asynchronicity of the event streaming architecture.

      So you would like to listen for this entity beeing created.
      Hopefully, you know its ID because you just created the post and this derived
      entity uses the same ID.

      So you can call the `subscribe_entiy/4` function to listen for the events coming from
      this derived entity.

      ```elixir
      subscribe_entity(socket, "post_with_comments_count", :direct, %{"id" => post_id})
      ```

      ## Params

      * `socket`: The socket
      * `entity_name`: The entity name to listen to as a string
      * `subscription`: The kind of channel you want to listen to (these are the same as for the module configuration: `:direct`, `:unscoped`, `%{scopes: []}`)
      * `params`: A map with the parameters needed for `:direct` and `:scopes` scopes.

      ## Return value

      The updated `socket`

      ## Received events

      Events are received the same way as other events but related to the entity:

      ```elixir
      @impl true
      def handle_info({:on_insert, :direct, "post_with_comments_count", post}, socket) do
        {:noreply, socket |> push_navigate(to: "/posts/\#{post.id}")}
      end
      ```

      """
      def subscribe_entity(
            socket,
            entity_name,
            subscription,
            params \\ %{}
          ) do
        handle_subscriptions(
          fn channel ->
            [adapter: pubsub_adapter, name: pubsub] =
              unquote(application).get_env(:event_streamex, :pubsub)

            pubsub_adapter.subscribe(pubsub, channel)
          end,
          :subscribed,
          socket,
          entity_name,
          [subscription],
          unquote(source_modules),
          params,
          &Phoenix.LiveView.put_private/3
        )
      end

      @doc """
      Manually unsubscribes from an event's events.

      ## Params

      * `socket`: The socket
      * `entity_name`: The name of the entity to stop linstening
      * `subscription`: The kind of channel to stop listening to (these are the same as for the module configuration: `:direct`, `:unscoped`, `%{scopes: []}`)

      ## Return value

      The updated socket
      """
      def unsubscribe_entity(
            socket,
            entity_name,
            subscription
          ) do
        {_subscription_state, event_params} =
          get_subscribe_state(socket.private, entity_name)

        handle_subscriptions(
          fn channel ->
            [adapter: pubsub_adapter, name: pubsub] =
              unquote(application).get_env(:event_streamex, :pubsub)

            pubsub_adapter.unsubscribe(pubsub, channel)
          end,
          :unsubscribed,
          socket,
          entity_name,
          [subscription],
          unquote(source_modules),
          event_params,
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
    {subscription_state, event_params} = get_subscribe_state(socket.private, table_name)

    if check_subscribed_status(subscription_state.unscoped, type) do
      Logger.debug("#{source_modules} #{inspect(type)} channel: #{table_name}")

      # General channel for the resource
      subscriber.("#{table_name}")

      socket
      |> update_subscribe_state(
        put_private,
        table_name,
        set_unscoped_subscription_state(subscription_state, type),
        event_params
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
    {subscription_state, event_params} = get_subscribe_state(socket.private, table_name)

    if check_subscribed_status(subscription_state.direct, type) do
      Logger.debug("#{source_modules} #{inspect(type)} channel: #{table_name}/#{id}")

      # Channel for direct access to the resource
      subscriber.("#{table_name}/#{id}")

      socket
      |> update_subscribe_state(
        put_private,
        table_name,
        set_direct_subscription_state(subscription_state, type),
        update_event_params(event_params, params, ["id"])
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
    {subscription_state, event_params} = get_subscribe_state(socket.private, table_name)

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
        |> update_subscribe_state(
          put_private,
          table_name,
          set_scoped_subscription_state(subscription_state, %{scopes: scopes}, type),
          update_event_params(
            event_params,
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
  defp handle_subscription(
         _subscriber,
         _type,
         socket,
         _table_name,
         _sub_type,
         _source_modules,
         _params,
         _put_private
       ) do
    socket
  end

  @doc false
  def get_subscribe_state(private, entity) do
    entity_state =
      private
      |> Map.get(:subscriptions, %{})
      |> Map.get(entity, %{subscribed?: default_subscribed_state(), event_params: %{}})

    {Map.get(entity_state, :subscribed?, default_subscribed_state()),
     Map.get(entity_state, :event_params, %{})}
  end

  @doc false
  def update_subscribe_state(socket, put_private, entity, new_state, event_params) do
    entity_state = %{subscribed?: new_state, event_params: event_params}

    subs =
      socket.private
      |> Map.get(:subscriptions, %{})

    socket
    |> put_private.(:subscriptions, subs |> Map.put(entity, entity_state))
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
  defp deserialize_scopes(scopes) do
    %{
      scopes:
        scopes
        |> String.split("/")
        |> Enum.map(fn scope ->
          [field | [entity | []]] = scope |> String.split(":")

          {String.to_atom(field), entity}
        end)
    }
  end

  @doc false
  def update_event_params(event_params, params, keys) do
    Enum.reduce(keys, event_params, &Map.put(&2, &1, Map.get(params, &1, nil)))
  end

  defp check_subscribed_status(subscribed?, :subscribed), do: not subscribed?
  defp check_subscribed_status(subscribed?, :unsubscribed), do: subscribed?

  @doc false
  def subscribed_entities(private) do
    private
    |> Map.get(:subscriptions, %{})
    |> Map.keys()
  end

  @doc false
  def get_entity_current_subscriptions(private, entity) do
    {subscription_state, _params} = get_subscribe_state(private, entity)

    subscription_state
    |> Map.to_list()
    |> Enum.filter(&elem(&1, 1))
    |> Enum.map(fn
      {:direct, _} -> :direct
      {:unscoped, _} -> :unscoped
      {scopes, _} -> deserialize_scopes(scopes)
    end)
  end
end
