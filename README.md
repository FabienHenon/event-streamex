# EventStreamex

Package to add event streaming in your Elixir application, using PostgreSQL WAL.

Works very well with [Phoenix LiveView](https://hexdocs.pm/phoenix_live_view/welcome.html) to add realtime to your app.

This package uses the [Write-Ahead-Log from PostgreSQL](https://www.postgresql.org/docs/current/wal-intro.html) to listen for changes in database tables and then, handle them in your application in 2 ways:

- To add realtime support in your views
- To update other entities in realtime (as soon as an entity it depends on is updated)

Full documentation can be found [here](https://hexdocs.pm/event_streamex/1.0.0/EventStreamex.html).

## Usage

### Installation

The package can be installed by adding `event_streamex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:event_streamex, "~> 1.0.0"}
  ]
end
```

### Configuration

Here are the configuration parameters for `EventStreamex` with their default value:

```elixir
config :event_streamex,
  app_name: nil,
  database: "postgres",
  durable_slot: true,
  error_logger_adapter: {EventStreamex.Operators.Logger.LoggerAdapter, []},
  hostname: "localhost",
  operator_queue_backoff_multiplicator: 2,
  operator_queue_max_restart_time: 10000,
  operator_queue_max_retries: 5,
  operator_queue_min_restart_time: 500,
  password: "postgres",
  port: "5432",
  process_status_entity_field: :updated_at,
  process_status_storage_adapter: {EventStreamex.Operators.ProcessStatus.DbAdapter, []},
  publication: "events",
  pubsub: [adapter: Phoenix.PubSub, name: nil],
  queue_storage_adapter: {EventStreamex.Operators.Queue.DbAdapter, []},
  slot_name: "postgres_slot",
  url: "",
  username: "postgres"
```

- `app_name`: Your application name
- `database`: Name of the database to connect to
- `durable_slot`: Is the replication slot for the WAL temporary or durable? If you set it to `false`, then, you will loose all WAL events sent while your application was not running.
- `error_logger_adapter`: The logger module used with the `start_link` parameter to send it
- `hostname`: Your PostgreSQL hostname
- `operator_queue_backoff_multiplicator`: Multiplicator used to compute the new wait time after an operator crash (See `EventStreamex.Operators.Logger.ErrorLoggerAdapter`)
- `operator_queue_max_restart_time`: Maximum time to wait in milliseconds before the restart of a crashed operator (See `EventStreamex.Operators.Logger.ErrorLoggerAdapter`)
- `operator_queue_max_retries`: Maximum number of restarts of a crashed operator (See `EventStreamex.Operators.Logger.ErrorLoggerAdapter`)
- `operator_queue_min_restart_time`: Time to wait in milliseconds before the first restart of a crashed operator (See `EventStreamex.Operators.Logger.ErrorLoggerAdapter`)
- `password`: Password used to connect to the database
- `port`: Port of the database
- `process_status_entity_field`: The field name used is your entities to represent the last updated time. It must be set also when the entity is created. And it should have a precision of microseconds (use the `utc_datetime_usec` type with `Ecto`). This field is used to update the `EventStreamex.Operators.ProcessStatus` registry.
- `process_status_storage_adapter`: Module used for process status storage and the parameter sent to the `start_link` function
- `publication`: Name of the publication used by the WAL (it is created automatically if it does not already exist)
- `pubsub`: Adapter to use for pubsub and its name
- `queue_storage_adapter`: Module used for queue storage and the parameter sent to the `start_link` function
- `slot_name`: Name of the replication slot for the WAL. It is automatically created if it does not exist.
- `url`: URL to connect to the database. If it is not set, the other parameters will be used instead
- `username`: Username to connect to the database

For the package to work correctly you will have to specify, at least:

- The database connection parameters
- The `:app_name`
- The `pubsub` `:name`

### Mark the entity for listening

To mark an entity for listening, you will have to use the `EventStreamex.Events` module:

```elixir
defmodule MyApp.Blog.Post do
  use MyApp.Schema

  use EventStreamex.Events, name: :my_app, schema: "posts"

  import Ecto.Changeset

  alias MyApp.Blog.PostWithCommentsCount

  schema "posts" do
    field :title, :string
    field :body, :string

    timestamps(type: :utc_datetime)
  end

  @doc false
  def changeset(post, attrs) do
    post
    |> cast(attrs, [:id, :title, :body])
    |> validate_required([:title, :body])
  end
end
```

Any marked entity will tell PostreSQL's WAL to emit events as soon as it is `inserted`, `updated`, `deleted`.

### Listen to events in live views

Now that our `posts` entity is subscribed to, in the WAL, we can listen to its changes in a live view:

```elixir
defmodule MyApp.PostLive.Index do
  use MyApp, :live_view

  use EventStreamex.EventListener,
    schema: "post_with_comments_counts",
    subscriptions: [:unscoped]

  alias MyApp.Blog
  alias MyApp.Blog.Post

  @impl true
  def mount(params, session, socket) do
    {:ok, socket} = super(params, session, socket)

    {:ok, stream(socket, :posts, Blog.list_posts())}
  end

  @impl true
  def handle_params(params, url, socket) do
    {_res, socket} = super(params, url, socket)

    {:noreply, socket}
  end

  @impl true
  def handle_info({:on_insert, [], "post_with_comments_counts", post}, socket) do
    {:noreply,
     socket
     |> stream_insert(:posts, post.new_record)
  end

  @impl true
  def handle_info({:on_update, [], "post_with_comments_counts", post}, socket) do
    {:noreply,
     socket
     |> stream_insert(:posts, post.new_record)
  end

  @impl true
  def handle_info({:on_delete, [], "post_with_comments_counts", post}, socket) do
    {:noreply,
     socket
     |> stream_delete(:posts, post.old_record)
  end
end
```

Here, we listen to `posts` events in our live view.

The `handle_info/2` callbacks are called with the kind of event received (`:on_insert`, `:on_update`, `:on_delete`) and the event.

You can, then do whatever you need to update your view.

### Events structure

An event looks like this:

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

_I am using the [WalEx package internally](https://github.com/cpursley/walex)_

- `name`: The name of the entity (ie: `:comments` for a table named `comments`)
- `type`: The type of event between `insert`, `update` and `delete`
- `source`: Information about the event:
  - `name`: "WalEx"
  - `version`: Current version of `WalEx`
  - `db`: The name of the database
  - `schema`: Mostly `"public"`
  - `table`: The name of the table (ie: `"comments"`)
  - `columns`: A map of fields with their type (ie: `%{"id": "integer", "message": "varchar"}`)
- `new_record`: The entity itself for `insert` and `update` events. `nil` for `delete` events.
- `old_record`: The entity itself for `delete` events. `nil` for `insert` and `update` events.
- `changes`: A map with the changes in the entity in `update` events, `nil` otherwise (see below)
- `timestamp`: The timstamp of the event in `DateTime` type
- `lsn`: A tuple containing information about the publication cursor

#### `changes`

When you receive an `update` event, you will also have the `changes` field set to a map containing
the changes the entity received since the update.

This map contains the changed fields as keys, and a map describing the change as value.
This "change" map contains 2 fields:

- `old_value`: The value before the update
- `new_value`: The value after the update

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

### Update other entities based on events

Now, let's assume that we also have a `comments` entity linked to a post,
and that we want to display our posts with the number of comments they have.

In event streaming we will create another entity (let's say `post_with_comments_count`), and update it when we receive events from `posts` and `comments`.

This entity will look like this:

- `id`: It's id (same one as the post id)
- `title`: Same one as the post title
- `body`: Same one as the post body
- `comments_count`: The number of comments of the post

```elixir
defmodule MyApp.Operators.PostWithCommentsCountOperator do
  use EventStreamex.Operators.Operator,
    schema: "post_with_comments_counts",
    repo: MyApp.Repo

  on_event(
    all_events(),
    entities(["comments", "posts"]),
    query(
      """
      MERGE INTO post_with_comments_counts dest
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
            comments_count = src.comments_count;
      """,
      id([{"comments", :post_id}, {"posts", :id}]),
      delete_with(["posts"])
    )
  )
end
```

Using an operator will allow us to insert/update/delete an entity based on events.

Here, we listen to `posts` and `comments` and, for each event, we execute the `MERGE` query.

This query will create the `post_with_comments_count` when a post is created, and update it when the post is updated.
It will also update the `comments_count` each time there is a new comment,
or a comment has been deleted.

The `$1` parameter in the query will be replaced by the field in the listened entities that corresponds to the id of the `post_with_comments_count`.
In this example, it's the post ID. Which is found in the `post_id` field in `comments`.
That comes from this line: `id([{"comments", :post_id}, {"posts", :id}])`.

`delete_with(["posts"])` will tell the operator that a post deletion must also delete the `post_with_comments_count`.

If you can more complex scenarios, you can also use a function instead of `query/3`:

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

## Why using event streaming?

Although using event streaming with `EventStreamex` looks easy, why bother using event streaming in the first place?

### The issue without event streaming

Let's say you have a `posts` entity which contains:

- `title`: a title
- `body`: a body

You also have a `comments` entity which contains:

- `post_id`: The ID of the post it is attached to
- `comment`: The comment content

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

#### Your queries will become slower and slower over time

Because subqueries are not free. They take more time to execute that a simple query with no join and subquery.

So, the more posts and comments you have, the slower the query will be.

#### You will end up adding complexity in your queries

As your app evolves, you will probably create new entities related to your posts and comments.
Thus, you will add more and more subqueries and joins to your queries, making them slower and harder to maintain...

#### Realtime will become a mess to maintain

If you want to handle realtime events like updating the number of comments when a new one is created it will be a mess to maintain overtime because you will end up subscribing to a lot of events as you link your posts to other entities.

And you will probably forget to subscribe to some of the events making your app inconsistant (some features will be in realtime and other won't be).

#### Almost impossible to maintain recursivity

If your comments can have subcomments it will be almost impossible
to maintain the comments count because of the recursivity it implies.

#### Depending on entities not directly related to the base entity

Let's say you create another entity named `ratings` which saves each rating given to a specific comment.

It would look like this:

- `comment_id`: The ID of the comment related to this rating
- `rating`: The given rating

Now imagine you want to display, for every comment, the average rating and the total count of ratings for this comment.

Here again, you could solve this with some subqueries.

But what if you want to show the average rating of all comments in the post
so that users can see that a post has received a lot of useful comments.

I don't even want to think about the query that would require...

### Benefits from using event streaming

Let's come back to our `posts` and `comments`.
Now let's imagine that instead of our subquery to count the comments we use
an event streaming architecture and create another entity whose goal is
specifically to show the posts with their comments count.

This `post_with_comments_counts` entity will look like that:

- `title`: a title
- `body`: a body
- `comments_count`: the number of comments of the post

And this entity will listen to new comments to update its comments count:
when a new comment is created it will increase the number of comments of the related post by 1.

With `EventStreamex` it would look like this:

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

And each time a post or a comment is inserted, updated, deleted, we perform this `MERGE` query to create/update our `post_with_comments_counts`.

The `MERGE` query will insert a new post if the post does not exist and update it if it already exists.

`id/1` is used to map the fields of the listened entities to the id of the
`post_with_comments_counts` entity (`id` for a post and `post_id` for a comment).

Finally, `delete_with/1` tells the operator which entities deletion should
also delete the related `post_with_comments_counts` entity.

As you can see, this `MERGE` query also uses a subquery, so why is it better than our first implementation?

#### This is a naive implementation

The `query/3` macro is provided for simplicity.
But you can also replace it by a function to do things as you think it's best (optimize the queries according to the type of event and entity received).

This function will receive the event, allowing you to do whatever you want with it.

#### The query is executed in the background

In our first example, the query was executed everytime someone wants to display the list of posts.

In our event streaming example, the query is executed in the background each time there is an event with a post or a comment.

To display your list of posts you will just have to do:

```sql
SELECT * FROM post_with_comments_counts;
```

Which is simpler and faster than our former query.

#### Realtime is easy

You can use the module `EventStreamex.EventListener` to easily add realtime
in your live view applications.

Because we are using PostgreSQL WAL events, everytime something happens on an entity (`post_with_comments_counts` as well), an event is sent.

This event will be processed by operators listening to it (like our operator here does for `posts` and `comments`), and it will also be sent to a pubsub processor.

The `EventStreamex.EventListener`, now just has to listen to this pubsub processor to notify your live view that the entity your are showing as been updated.

Everything, now, can be realtime, without beeing hard to maintain.

#### A better pattern to model your data

With this approach you keep your entities consistent and simple.
And you create specialized entities for what you want to show in your views.

A post will stay a post with its `title` and `body` fields and a comment will stay a comment with a `comment` field.

`post_with_comments_count` is just a specialized entity used for your view.

This keeps your data and APIs consistent and simple with explicit fields and names.

For instance, let's say you have a `users` entity with a `login` and a `password` field.
But later on, you realize that you need to store some information about your users, like its name, its address, and so on.

You will be tempted to update your `users` entity to add these new fields:

- `login`
- `password`
- `address`
- `name`

But still, in your application, when comes the moment to create your user, you only need the `login` and `password`.

So what do you do? You set the other fields as optionals in your API and set them a default value (empty string or null).

And in the view that allows a user to specify more information about him (`name` and `address`), you will either:

- Use the same API endpoint as for the `users` creation
- Or create a new dedicated API endpoint

But both approachs are bad practices!

In the first approach that means that your API expects 2 mandatory fields: `login` and `password`, and 2 optional fields: `name` and `address`.
But here, there is no sense in sending the `login` and `password`.
And maybe you would like the `name` and `address` fields to be mandatory.
But because your API set these fields as optionals, you will have to check for them from the client side.
Which is a bad practice.

In the second approach, that means that you will create an API endpoint for the user profile.
But this API, will finally update the `users` entity.
You end up with hard to understand APIs because their name does not match
to the entity being modified.

A better approach would be to have a `users` entity with:

- `login`
- `password`

And its own API endpoint `/users` with only these two fields.

And you would have a `user_profiles` entity with:

- `user_id`
- `name`
- `address`

And its own API endpoint `/users/{user_id}/user_profiles` with only the `name` and `address` fields.

And what if you need to display all of these information in your view?
Well, you use an operator like we did for the `post_with_comments_counts`.
Named, for instance: `user_with_profiles`, and listening to `users` and `user_profiles` events.

This way, you keep your data and API consistent.

## How to handle asynchronicity of entity creation

Event streaming is nice, but it makes create/update/delete operations asynchronous for derived entities.
Which can be a nightmare to handle correctly.

Let's say you have a LiveView application showing a list of `post_with_comments_counts`, which is a derived entity from the `posts` entity, containing, in addition to `posts` fields, a `comments_count` field.

From this view, we can also create a new post.
But we don't create a `post_with_comments_counts` directly.
Instead, we must create a `posts` entity.
Which, in turn, will trigger the creation of the `post_with_comments_counts` using event streaming.

What we want to do is to redirect to the post detail page when the post is
created.

But we have a problem!

Because the creation of the `posts` entity is synchronous, we can redirect
as soon as it is created.
But that does not mean the `post_with_comments_counts` entity will be created already... while it's this entity we need to show.

To handle this scenario and be able to display the `post_with_comments_counts` entity as soon as it is created we have 3 solutions:

### Wait for the entity to be created before redirecting

To ensure you redirect to an entity that exists, you can actualy wait for it
to be created.

In order to do so, you will have to listen to this entity to know when it's been created.

You can use the `subscribe_entity/4` function for that matter.

```elixir
defmodule MyApp.PostLive.Index do
  use MyApp, :live_view

  use EventStreamex.EventListener,
    schema: "post_with_comments_counts",
    subscriptions: [:unscoped]

  alias MyApp.Blog
  alias MyApp.Blog.Post

  @impl true
  def mount(params, session, socket) do
    {:ok, socket} = super(params, session, socket)

    {:ok, stream(socket, :posts, Blog.list_posts())}
  end

  @impl true
  def handle_params(params, url, socket) do
    {_res, socket} = super(params, url, socket)

    {:noreply, apply_action(socket, socket.assigns.live_action, params)}
  end

  defp apply_action(socket, :new, _params) do
    socket
    |> assign(:page_title, "New Post")
    |> assign(:post, %Post{})
  end

  defp apply_action(socket, :index, _params) do
    socket
    |> assign(:page_title, "Listing Posts")
    |> assign(:post, nil)
  end

  @impl true
  def handle_info({MyApp.PostLive.FormComponent, {:saved, post}}, socket) do
    {:noreply,
     socket
     |> subscribe_entity("post_with_comments_counts", :direct, %{"id" => post.id})}
  end

  @impl true
  def handle_info({:on_insert, :direct, "post_with_comments_counts", post}, socket) do
    {:noreply,
     socket
     |> unsubscribe_entity("post_with_comments_counts", :direct)
     |> push_navigate(to: "/posts/#{post.new_record.id}")}
  end

  @impl true
  def handle_info({_, :direct, "post_with_comments_counts", _post}, socket) do
    {:noreply, socket}
  end

  @impl true
  def handle_info({:on_insert, [], "post_with_comments_counts", post}, socket) do
    {:noreply,
     socket
     |> stream_insert(:posts, post.new_record)
  end

  @impl true
  def handle_info({:on_update, [], "post_with_comments_counts", post}, socket) do
    {:noreply,
     socket
     |> stream_insert(:posts, post.new_record)
  end

  @impl true
  def handle_info({:on_delete, [], "post_with_comments_counts", post}, socket) do
    {:noreply,
     socket
     |> stream_delete(:posts, post.old_record)
  end
end
```

Here, our live view listens for `:unscoped` `post_with_comments_counts` entity for the `index` view in realtime (nothing new).

This code is more interesting:

```elixir
@impl true
def handle_info({MyApp.PostLive.FormComponent, {:saved, post}}, socket) do
  {:noreply,
    socket
    |> subscribe_entity("post_with_comments_counts", :direct, %{"id" => post.id})}
end
```

This function gets called when a `posts` has been created.
And inside it, instead of redirecting immediately, we call the `subscribe_entity/4` function to listen for `post_with_comments_counts` with a specific ID. The ID of the newly created post.

That means that as soon as this `post_with_comments_counts` entity will be created, we will be notified:

```elixir
@impl true
def handle_info({:on_insert, :direct, "post_with_comments_counts", post}, socket) do
  {:noreply,
    socket
    |> unsubscribe_entity("post_with_comments_counts", :direct)
    |> push_navigate(to: "/posts/#{post.new_record.id}")}
end
```

When the entity is created, we unsubscribe from the channel (this is optional is it will automatically unsubscribe as soon as the live view terminates), and we redirect to the detail page.

And we know, at this moment, the `post_with_comments_counts` entity will exist.

### Handle entity not found

Another solution is to redirect immediately without waiting for the `post_with_comments_counts` to be created, and by handling the case where the entity is not found yet.

```elixir
defmodule MyApp.PostLive.Show do
  use MyApp, :live_view

  use EventStreamex.EventListener,
    schema: "post_with_comments_counts",
    subscriptions: [:direct]

  alias MyApp.Blog

  @impl true
  def mount(params, session, socket) do
    super(params, session, socket)
  end

  @impl true
  def handle_params(%{"id" => id} = params, url, socket) do
    {_res, socket} = super(params, url, socket)

    post = Blog.get_post(id)

    {:noreply,
     socket
     |> assign(:id, id)
     |> assign(:post, post)
  end

  @impl true
  def handle_info({:on_insert, :direct, "post_with_comments_counts", post}, socket) do
    {:noreply,
     socket
     |> assign(:post, post.new_record)}
  end

  @impl true
  def handle_info({:on_update, :direct, "post_with_comments_counts", post}, socket) do
    {:noreply,
     socket
     |> assign(:post, post.new_record)}
  end

  @impl true
  def handle_info({:on_delete, :direct, "post_with_comments_counts", _post}, socket) do
    {:noreply, socket |> put_flash(:warning, "Ce post vient d'être supprimé")}
  end
end
```

Here we fecth the entity (but without triggering an exception if it does not exist), and we replace it if we receive the `:on_insert` event for that entity.

You will just have to ensure your view shows a loader or something until the entity is created.

### Handling async with APIs

We saw 2 solutions we could use with a live view.
But what if you develop an mobile app and use an API?

If you have an API endpoint to create a new `posts` (`POST /posts`), and as soon as your appliation calls it, it redirects to another view requesting the detail of the created `post_with_comments_counts` (`GET /post_with_comments_counts`), then, you may receive a 404 error because of the async creation of the `post_with_comments_counts` entity.

In order to avoid that, you could implement a mecanism that would return, along with the `posts` newly created, the post creation timestamp.

Then, in the `GET /post_with_comments_counts` endpoint, you would send along the timestamp you got from the `POST /posts` endpoint.

And in your controller you would check if the entity has been processed before returning the `post_with_comments_counts` requested.

You can use `EventStreamex.Operators.ProcessStatus.processed?/2` in order to do so.

For instance, when you create a `posts`, you receive this header from the API:

```
X-Event-Streamex-Entity-Timestamp: "posts/1422057007123000"
```

_(You can use the `:process_status_entity_field` config parameter to set the field to use in your entities to know the last updated time. You should return the field's value in your API, and use this value in the `processed?/2` function)_

You will send this header when you call the `GET /post_with_comments_counts` endpoint.

And the controller will immediately check if the entity has been processed:

```elixir
EventStreamex.Operators.ProcessStatus.processed?("posts", 1422057007123000)
```

If the entity has not been processed yet, then, you know the `post_with_comments_counts` entity has not been created yet (it works the same for updated entities).
So you can immediately return a 425 response for instance, informing the client to retry later.

If the entity has been processed, then you fetch it and return it.
If you get a 404 error, this time, that means the entity did not exist.

## Tests

To test the package you can run:

```
mix test
```

Some tests will need access to a PostgreSQL database.

Here are the credentials used by the tests:

- Database: `postgres`
- Hostname: `locahost`
- Password: `""`
- Port: `5432`
- Username: `postgres`
