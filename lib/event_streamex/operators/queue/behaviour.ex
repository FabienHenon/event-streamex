defmodule EventStreamex.Operators.Queue.QueueStorageAdapter do
  @moduledoc """
  Behaviour for the queue storage.

  Every event received is dispatched to operators (`EventStreamex.Operators.Operator`),
  and executed sequentially to keep ordenancing.

  Every time an event is received, a task is added to the queue of events for each
  operator listening for this event.
  And everytime a an operator task is finished, it is removed from the queue.

  The adapters keeps a copy of this queue, mostly in case of crash (Except for `EventStreamex.Operators.Queue.MemAdapter` which keeps the queue only in memory and should not be used in production)

  There are currently 2 queue adapters:
  * `EventStreamex.Operators.Queue.DbAdapter`: A queue that uses the database tu store its items (This is the default queue adapter)
  * `EventStreamex.Operators.Queue.MemAdapter`: A queue that stores its items in memory

  You can create your own adapter and set the config to use it.

  Here is the full code of the memory adapter:

  ```elixir
  defmodule EventStreamex.Operators.Queue.MemAdapter do
    use GenServer
    @behaviour EventStreamex.Operators.Queue.QueueStorageAdapter

    @impl true
    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl EventStreamex.Operators.Queue.QueueStorageAdapter
    def add_item(item) do
      GenServer.call(__MODULE__, {:save, item})
    end

    @impl EventStreamex.Operators.Queue.QueueStorageAdapter
    def delete_item(item) do
      GenServer.call(__MODULE__, {:delete, item})
    end

    @impl EventStreamex.Operators.Queue.QueueStorageAdapter
    def load_queue() do
      GenServer.call(__MODULE__, :load)
    end

    @impl EventStreamex.Operators.Queue.QueueStorageAdapter
    def reset_queue() do
      GenServer.call(__MODULE__, :reset)
    end

    # Callbacks

    @impl true
    def init(_opts) do
      {:ok, []}
    end

    @impl true
    def handle_call({:save, item}, _from, queue) do
      new_queue = queue ++ [item]
      {:reply, {:ok, new_queue}, new_queue}
    end

    @impl true
    def handle_call({:delete, {id, _item}}, _from, queue) do
      new_queue = queue |> Enum.reject(fn {i, _t} -> i == id end)
      {:reply, {:ok, new_queue}, new_queue}
    end

    @impl true
    def handle_call({_action, nil}, _from, queue) do
      {:reply, {:ok, queue}, queue}
    end

    @impl true
    def handle_call(:load, _from, queue) do
      {:reply, {:ok, queue}, queue}
    end

    @impl true
    def handle_call(:reset, _from, _queue) do
      {:reply, {:ok, []}, []}
    end
  end
  ```

  The adapter must be a `GenServer` and declare the function `c:start_link/1`.

  Then, it must provide these functions:
  * `c:add_item/1`: Add a new item to the queue
  * `c:delete_item/1`: Delete an item from the queue
  * `c:load_queue/0`: Return the entire queue
  * `c:reset_queue/0`: Remove all items from the queue

  An item is a tuple containing:
  * The operator module that processes the event
  * The WalEx event (more on this structure in `EventStreamex.EventListener`)
  """

  @moduledoc since: "1.0.0"

  @typedoc """
  A queue item to store.

  An item is a tuple containing:
  * The operator module that processes the event
  * The WalEx event (more on this structure in `EventStreamex.EventListener`)
  """
  @type queue_item() :: {atom(), WalEx.Event.t()} | nil

  @typedoc """
  A list of `t:queue_item/0`
  """
  @type queue() :: [queue_item()]

  @doc """
  Callback called when a new item must be stored in the queue.

  The item is a `t:queue_item/0`.

  The return value is a result tuple.
  The value in case of success is not used for the moment.
  """
  @doc since: "1.0.0"
  @callback add_item(queue_item()) :: {:ok, term()} | {:error, term()}

  @doc """
  Callback called when an item has been processed and must be deleted from the queue.

  The item is a `t:queue_item/0`.

  The return value is a result tuple.
  The value in case of success is not used for the moment.
  """
  @doc since: "1.0.0"
  @callback delete_item(queue_item()) :: {:ok, term()} | {:error, term()}

  @doc """
  Called at startup or in case of processor crash to return the full list of items.

  The return value is a result tuple with the list of items `t:queue/0`.
  """
  @doc since: "1.0.0"
  @callback load_queue() :: {:ok, queue()} | {:error, term()}

  @doc """
  Called for tests to reset the queue

  The return value is a result tuple with an empty list.
  """
  @doc since: "1.0.0"
  @callback reset_queue() :: {:ok, queue()} | {:error, term()}

  @doc """
  Called when starting the queue adapter.

  The parameter is the one given in the config file when you set the adapter.

  The return value is a result with the `pid` of the adapter processor.
  """
  @doc since: "1.0.0"
  @callback start_link(any()) :: {:ok, pid()}

  use GenServer
  require Logger

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  def add_item(item) do
    GenServer.call(__MODULE__, {:save, item})
  end

  @doc false
  def delete_item(item) do
    GenServer.call(__MODULE__, {:delete, item})
  end

  @doc false
  def load_queue() do
    GenServer.call(__MODULE__, :load)
  end

  @doc false
  def reset_queue() do
    GenServer.call(__MODULE__, :reset)
  end

  # Callbacks

  @doc false
  @impl true
  def init({queue_storage_adapter, args}) do
    Logger.debug("QueueStorageAdapter starting...")
    {:ok, _pid} = queue_storage_adapter.start_link(args)
    Logger.debug("QueueStorageAdapter started")
    {:ok, queue_storage_adapter}
  end

  @doc false
  @impl true
  def handle_call({:save, item}, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.add_item(item), queue_storage_adapter}
  end

  @doc false
  @impl true
  def handle_call({:delete, item}, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.delete_item(item), queue_storage_adapter}
  end

  @doc false
  @impl true
  def handle_call(:load, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.load_queue(), queue_storage_adapter}
  end

  @doc false
  @impl true
  def handle_call(:reset, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.reset_queue(), queue_storage_adapter}
  end
end
