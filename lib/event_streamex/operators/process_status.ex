defmodule EventStreamex.Operators.ProcessStatus do
  @moduledoc """
  Handle the process status of entities.

  Everytime an event is processed, it's saved in the `ProcessStatus` along
  with the processed timestamp.

  Thus, the process status contains the latest processed timestamp for all entities.

  You can use this module to ensure an entity has been processed.

  Let's say you have `posts` and `post_with_comments_counts` entities.
  When a `posts` is created, an operator, listening for `posts` will create
  a `post_with_comments_counts` entity with the same ID.

  You have an API endpoint to create a new `posts` (`POST /posts`), and as soon as your appliation
  calls it, it redirects to another view requesting the detail of the created `post_with_comments_counts` (`GET /post_with_comments_counts`).

  But the creation of the later is asynchronous, so you may receive a 404 error from your API.

  In order to avoid that, you could implement a mecanism that would return, along
  with the `posts` newly created, the post creation timestamp.

  Then, in the `GET /post_with_comments_counts` endpoint, you would send along the timestamp you got
  from the `POST /posts` endpoint.

  And in your controller you would check if the entity has been
  processed before returning the `post_with_comments_counts` requested.

  You can use `processed?/2` in order to do so.

  For instance, when you create a `posts`, you receive this header from the API:

  ```
  X-Event-Streamex-Entity-Timestamp: "posts/1422057007123000"
  ```
  _(You can use the `:process_status_entity_field` config parameter to set
  the field to use in your entities to know the last updated time.
  You should return the field's value in your API, and use this value
  in the `processed?/2` function)_

  You will send this header when you call the `GET /post_with_comments_counts` endpoint.

  And the controller will immediately check if the entity has been processed:

  ```elixir
  EventStreamex.Operators.ProcessStatus.processed?("posts", 1422057007123000)
  ```

  If the entity has not been processed yet, then, you know the `post_with_comments_counts` entity
  has not been created yet (it works the same for updated entities).
  So you can immediately return a 425 response for instance, informing the client to retry later.

  If the entity has been processed, then you fetch it and return it.
  If you get a 404 error, this time, that means the entity did not exist.
  """
  @moduledoc since: "1.1.0"
  use GenServer
  alias EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter

  require Logger

  @ets_name :process_status

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns the last timestamp when the specified entity has been processed

  ## Params

  The entity to check

  ## Returns

  The last time this entity was processed
  """
  @doc since: "1.1.0"
  def last_processed(entity) do
    case :ets.lookup(@ets_name, entity) do
      [{^entity, timestamp}] ->
        timestamp

      _ ->
        nil
    end
  end

  @doc """
  Has the entity with the specified timestamp already been processed?

  ## Params

  * `entity`: The entity to check
  * `timestamp`: Usually the `update_at` field of the entity

  ## Returns

  `true` if this entity with this timestamp, or a later timestamp has been processed.
  `false` otherwise.
  """
  @doc since: "1.1.0"
  def processed?(entity, timestamp) do
    case last_processed(entity) do
      nil -> false
      last_timestamp -> last_timestamp >= timestamp
    end
  end

  @doc """
  Updates the processed timestamp of an entity **(do not use this function yourself)**.
  """
  @doc since: "1.1.0"
  def entity_processed(entity, timestamp) do
    GenServer.call(__MODULE__, {:save, entity, timestamp})
  end

  @doc """
  Removes all items from the process status.

  **Should only be used for testing purposes**
  """
  @doc since: "1.1.0"
  def reset() do
    GenServer.call(__MODULE__, :reset)
  end

  # Callbacks

  @doc false
  @impl true
  def init(_opts) do
    Logger.debug("ProcessStatus starting...")

    {:ok, process_status} = ProcessStatusStorageAdapter.load()

    table = :ets.new(@ets_name, [:set, :protected, :named_table, read_concurrency: true])

    :ets.insert(@ets_name, process_status)

    Logger.debug("ProcessStatus started...")

    {:ok, table}
  end

  @doc false
  @impl true
  def handle_call({:save, entity, timestamp}, _from, table) do
    :ets.insert(@ets_name, {entity, timestamp})
    ProcessStatusStorageAdapter.item_processed({entity, timestamp})
    {:reply, :ok, table}
  end

  @doc false
  @impl true
  def handle_call(:reset, _from, table) do
    :ets.delete_all_objects(@ets_name)
    ProcessStatusStorageAdapter.reset()
    {:reply, :ok, table}
  end
end
