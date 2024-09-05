defmodule EventStreamex.Operators.Operator do
  @moduledoc """
  An operator listens for events and will usually create/update/delete
  another entity based on those events.

  This is a very usefull pattern. Here is why:

  ## The issue without event streaming

  Let's say you have a `posts` entity which contains:
  * `title`: a title
  * `body`: a body

  You also have a `comments` entity which contains:
  * `post_id`: The ID of the post it is attached to
  * `comment`: The comment content

  When you show a list of your posts you might want to show the number
  of comments it contains.

  To do so you will be tempted to make a SQL query with a subquery that will
  count the comments of your posts.

  This is pretty trivial:

  ```sql
  SELECT
    p.id AS id,
    p.title AS title,
    p.body AS body,
    (SELECT COUNT(*) FROM comments c WHERE c.post_id = p.id) AS comments_count
  FROM posts p;
  ```

  But it has several flaws.
  Let's see them in more details.

  ### Your queries will become slower and slower over time

  Because subqueries are not free. They take more time to execute that a simple
  query with no join and subquery.

  So, the more posts and comments you have, the slower the query will be.

  ### You will end up adding complexity in your queries

  As your app evolves, you will probably create new entities related to your posts and comments.
  Thus, you will add more and more subqueries and joins to your queries, making them slower and harder to maintain...

  ### Realtime will become a mess to maintain

  If you want to handle realtime events like updating the number of comments when a new one
  is created it will be a mess to maintain overtime because you will end up subscribing to a lot of events
  as you link your posts to other entities.

  And you will probably forget to subscribe to some of the events making your app
  inconsistant (some features will be in realtime and other won't be).

  ### Almost impossible to maintain recursivity

  If your comments can have subcomments it will be almost impossible
  to maintain the comments count because of the recursivity it implies.

  ### Depending on entities not directly related to the base entity

  Let's say you create another entity named `ratings` which saves each rating given
  to a specific comment.

  It would look like this:
  * `comment_id`: The ID of the comment related to this rating
  * `rating`: The given rating

  Now imagine you want to display for every comment, the average rating and
  the total count of ratings for this comment.

  Here again, you could solve this with some subqueries.

  But what if you want to show the average rating of all comments in the post
  so that users can see that a post has received a lot of useful comments.

  I don't even want to think about the query to build to do that...

  ## Benefits from using event streaming

  Let's come back to our `posts` and `comments`.
  Now let's imagine that instead of our subquery to count the comments we use
  an event streaming architecture and create another entity whose goal is
  specifically to show the posts with their comments count.

  This `post_with_comments_counts` entity will look like that:
  * `title`: a title
  * `body`: a body
  * `comments_count`: the number of comments of the post

  And this entity will listen to new comments to update its comments count:
  when a new comment is created it will increase the number of comments of the related post by 1.

  With `EventStreamex` it would look like that:

  ```elixir
  defmodule MyApp.Operators.PostWithCommentsCountOperator do
    use EventStreamex.Operators.Operator,
      schema: "post_with_comments_counts",
      repo: MyApp.Repo

    on_event(
      all_events(),
      entities(["comments", "posts"]),
      query(
        "MERGE INTO post_with_comments_counts dest
        USING (
            SELECT
                p.id AS id,
                p.title AS title,
                p.body AS body,
                p.inserted_at AS inserted_at,
                p.updated_at AS updated_at,
                (SELECT COUNT(c.id) FROM comments c WHERE c.post_id = p.id) AS comments_count
            FROM posts p
            WHERE p.id = $1
        ) src ON src.id = dest.id
        WHEN NOT MATCHED THEN
          INSERT (id, title, body, inserted_at, updated_at, comments_count)
          VALUES (src.id, src.title, src.body, src.inserted_at, src.updated_at, src.comments_count)
        WHEN MATCHED THEN
          UPDATE SET
              title = src.title,
              body = src.body,
              inserted_at = src.inserted_at,
              updated_at = src.updated_at,
              comments_count = src.comments_count;",
        id([{"comments", :post_id}, {"posts", :id}]),
        delete_with(["posts"])
      )
    )
  end
  ```

  Here, we use the `EventStreamex.Operators.Operator` module to listen for
  `posts` and `comments` in all kinds of events (insert, update, delete).

  And each time a post or a comment is inserted, updated, deleted, we perform this `MERGE` query
  to create/update our `post_with_comments_counts`.

  The `MERGE` query will insert a new post if the post does not exist and update
  it if it already exists.

  `id/1` is used to map the fields of the listened entities to the id of the
  `post_with_comments_counts` entity (`id` for a post and `post_id` for a comment).

  Finally, `delete_with/1` tells the operator which entities deletion should
  also delete the related `post_with_comments_counts` entity.

  As you can see, this `MERGE` query also uses a subquery, so why is it better
  than our first implementation?

  ### This is a naive implementation

  The `query/3` macro is provided for simplicity.
  But you can also replace it by a function to do things as you think it's best.

  This function will receive the event, allowing you to do whatever you want with it.

  ### The query is executed in the background

  In our first example, the query was executed everytime someone wants to
  display the list of posts.

  In our event streaming example, the query is executed in the background
  each time there is an event with a post or a comment.

  To display your list of posts you will just have to do:

  ```sql
  SELECT * FROM post_with_comments_counts;
  ```

  Which is simpler and faster than our former query.

  ### Realtime is easy

  You can use the module `EventStreamex.EventListener` to easily add realtime
  in your live view applications.

  Because we are using PostgreSQL WAL events, everytime something happens on an entity
  (`post_with_comments_counts` as well), an event is sent.

  This event will be processed by operators listening to it (like our operator here does for `posts` and `comments`),
  and it will be sent to a pubsub processor.

  The `EventStreamex.EventListener`, now just has to listen to this pubsub processor
  to notify your live view the entity your are showing as been updated.

  Everything, now, can be realtime, without beeing hard to maintain.

  ### A better pattern to model your data

  With this approach you keep your entities consistent and simple.
  And you create specialized entities for what you want to show in your views.

  A post will stay a post with its `title` and `body` fields and a comment will
  stay a comment with a `comment` field.

  `post_with_comments_count` is just a specialized entity used for your view.

  This keeps your data and APIs consistent and simple with explicit fields and names.

  For instance, let's say you have a `users` entity with a `login` and a `password` field.
  But later on, you realize that you need to store some information about your users,
  like its name, its address, and so on.

  You will be tempted to update your `users` entity to add these new fields:
  * `login`
  * `password`
  * `address`
  * `name`

  But still, in your application, when comes the moment to create your user,
  you only need the `login` and `password`.

  So what do you do? You set the other fields as optionals in your API and set them
  a default value (empty string or null).

  And in the view that allows a user to specify more information about him (`name` and `address`),
  you will either:
  * Use the same API endpoint as for the `users` creation
  * Or create a new dedicated API endpoint

  But both approachs are bad practices!

  In the first approach that means that your API expects 2 mandatory fields: `login` and `password`
  and 2 optional fields: `name` and `address`.
  But here, there is no sense in sending the `login` and `password`.
  And maybe you would like the `name` and `address` fields to be mandatory.
  But because your API set these fields as optionals, you will have to check for them from the client side.
  Which is a bad practice.

  In the second approach, that means that you will create an API endpoint for the user profile.
  But this API, will finally update the `users` entity.
  You end up with hard to understand APIs because their name does not match
  to the entity being modified.

  A better approach would be to have a `users` entity with:
  * `login`
  * `password`
  And its own API endpoint `/users` with only these two fields.

  And you would have a `user_profiles` entity with:
  * `user_id`
  * `name`
  * `address`
  And its own API endpoint `/users/{user_id}/user_profiles` with only the `name` and `address` fields.

  And what if you need to display all of these information in your view?
  Well, you use an operator like we did for the `post_with_comments_counts`.
  Named, for instance: `user_with_profiles`, and listening to `users` and `user_profiles` events.

  This way, you keep your data and API consistent.

  ## `use` params

  When you `use` this module, you will be able to specify these parameters:

  * `:schema`: The name of the entity as a string (mandatory field)
  * `:repo`: The repository used for the connection to the database (mandatory field)
  * `:sql_adapter`: The SQL adapter to use (Defaults to `Ecto.Adapters.SQL`)
  * `:id`: The field used as an ID for the `schema` (Defaults to `:id`)

  """
  @moduledoc since: "1.0.0"
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

  @typedoc """
  The event received by the operator

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
  """
  @typedoc since: "1.0.0"
  @type event() :: %WalEx.Event{
          name: atom(),
          type: :insert | :update | :delete,
          source: %WalEx.Event.Source{
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

  @doc """
  This macro is the one listening the events.

  You have to define one and only one in your operator.

  ## Parameters

  * `events`: A list of event types to listen to (`:insert`, `:update`, `:delete`). You can use the `all_events/0`, `events/1` and `event/1` macros to help you.
  * `entities`: A list of entities to list to (use their table name as string). You can use `entities/1` and `entity/1` macros to help you.
  * `callback`: A function that takes the event as parameter and must return a result. You can also use the `query/3` macro instead.

  ## Example

  ```elixir
  on_event(
    all_events(),
    entities(["comments", "posts"]),
    fn event ->
      {:ok, res} =
        event
        |> MyQuery.do_some_queries()
        |> MyQuery.then_another_query()
      {:ok, res}
    end
  )
  ```
  """
  @doc since: "1.0.0"
  @spec on_event(
          [:on_insert | :on_update | :on_delete],
          [binary()],
          (event() -> {:ok, any()} | {:error, any()})
        ) :: any()
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
            Process.sleep(10000)
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

  @doc """
  Macro used as first parameter to `on_event/3` to listen to all kinds of events:
  * `:on_insert`
  * `:on_update`
  * `:on_delete`
  """
  @doc since: "1.0.0"
  defmacro all_events(), do: quote(do: [:on_insert, :on_update, :on_delete])

  @doc """
  Macro used as first parameter to `on_event/3` to listen to a list of event types from these items:
  * `:on_insert`
  * `:on_update`
  * `:on_delete`
  """
  @doc since: "1.0.0"
  defmacro events(events), do: quote(do: unquote(events))

  @doc """
  Macro used as first parameter to `on_event/3` to listen to only one event type from these items:
  * `:on_insert`
  * `:on_update`
  * `:on_delete`
  """
  @doc since: "1.0.0"
  defmacro event(event), do: quote(do: [unquote(event)])

  @doc """
  Macro used as second parameter to `on_event/3` to listen to a list of entities.
  You have to use the name of the table in string.
  """
  @doc since: "1.0.0"
  defmacro entities(entities), do: quote(do: unquote(entities))

  @doc """
  Macro used as second parameter to `on_event/3` to listen to only one entity.
  You have to use the name of the table in string.
  """
  @doc since: "1.0.0"
  defmacro entity(entity), do: quote(do: [unquote(entity)])

  @doc """
  Macro used as third parameter to `on_event/3` to directly perform a query.

  ## Parameters

  * `query`: The query to execute for each event
  * `ids_mapping`: A list of tuples to map the correct entities' field to the id of the final entity. You can use the `id/1` macro.
  * `delete_with`: A list of entities that, when deleted, will also delete the final entity. You can use the `delete_with/1` and `no_delete/0` macros.

  ## Example

  ```elixir
  on_event(
    all_events(),
    entities(["comments", "posts"]),
    query(
      "MERGE INTO post_with_comments_counts dest
      USING (
          SELECT
              p.id AS id,
              p.title AS title,
              p.body AS body,
              p.inserted_at AS inserted_at,
              p.updated_at AS updated_at,
              (SELECT COUNT(c.id) FROM comments c WHERE c.post_id = p.id) AS comments_count
          FROM posts p
          WHERE p.id = $1
      ) src ON src.id = dest.id
      WHEN NOT MATCHED THEN
        INSERT (id, title, body, inserted_at, updated_at, comments_count)
        VALUES (src.id, src.title, src.body, src.inserted_at, src.updated_at, src.comments_count)
      WHEN MATCHED THEN
        UPDATE SET
            title = src.title,
            body = src.body,
            inserted_at = src.inserted_at,
            updated_at = src.updated_at,
            comments_count = src.comments_count;",
      id([{"comments", :post_id}, {"posts", :id}]),
      delete_with(["posts"])
    )
  )
  ```

  Your query will have a `$1` parameter which will correspond to the id of the final entity.
  This id is taken from the entity who emitted the event, based on the `ids_mapping` field.

  For instance, for a `comments` event, the final entity will use the `post_id` field as his own id as set in
  the `ids_mapping` field.

  This mapping is also used in the deletion of the entity.
  """
  @doc since: "1.0.0"
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

  @doc """
  Sets the mapping of ids.

  The mapping is a list of tuples containing:
  * The entity name (as string)
  * The field that corresponds to the final entity's id

  This macro is used as second argument to the `query/3` macro.

  ## Example

  ```elixir
  id([{"comments", :post_id}, {"posts", :id}])
  ```
  """
  @doc since: "1.0.0"
  defmacro id(mapping) when is_list(mapping), do: quote(do: unquote(mapping))

  @doc """
  Sets the entities that must delete the final entity when deleted.

  It takes a list of entities (as a list of strings).

  This macro is used as third argument to the `query/3` macro.

  ## Example

  ```elixir
  delete_with(["posts"])
  ```
  """
  @doc since: "1.0.0"
  defmacro delete_with(schemas) when is_list(schemas), do: quote(do: unquote(schemas))

  @doc """
  Specifies that no entity should delete the final entity when deleted.

  This macro is used as third argument to the `query/3` macro.
  """
  @doc since: "1.0.0"
  defmacro no_delete(), do: quote(do: [])

  @doc false
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

  @doc false
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

  @doc false
  def get_modules_for_event(event_type, schema) do
    registered_operators()
    |> Enum.filter(&(&1 |> OperatorsProtocol.schemas() |> Enum.member?(schema)))
    |> Enum.filter(&(&1 |> OperatorsProtocol.event_types() |> Enum.member?(event_type)))
  end
end
