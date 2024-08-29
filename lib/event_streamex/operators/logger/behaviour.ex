defmodule EventStreamex.Operators.Queue.QueueStorageAdapter do
  @type queue_item() :: {atom(), WalEx.Event.t()} | nil
  @type queue() :: [queue_item()]

  @callback add_item(queue_item()) :: {:ok, term()} | {:error, term()}

  @callback delete_item(queue_item()) :: {:ok, term()} | {:error, term()}

  @callback load_queue() :: {:ok, queue()} | {:error, term()}

  @callback reset_queue() :: {:ok, queue()} | {:error, term()}

  @callback start_link(any()) :: {:ok, pid()}

  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def add_item(item) do
    GenServer.call(__MODULE__, {:save, item})
  end

  def delete_item(item) do
    GenServer.call(__MODULE__, {:delete, item})
  end

  def load_queue() do
    GenServer.call(__MODULE__, :load)
  end

  def reset_queue() do
    GenServer.call(__MODULE__, :reset)
  end

  # Callbacks

  @impl true
  @spec init({atom(), any()}) :: {:ok, atom()}
  def init({queue_storage_adapter, args}) do
    Logger.debug("QueueStorageAdapter starting...")
    {:ok, _pid} = queue_storage_adapter.start_link(args)
    Logger.debug("QueueStorageAdapter started")
    {:ok, queue_storage_adapter}
  end

  @impl true
  def handle_call({:save, item}, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.add_item(item), queue_storage_adapter}
  end

  @impl true
  def handle_call({:delete, item}, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.delete_item(item), queue_storage_adapter}
  end

  @impl true
  def handle_call(:load, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.load_queue(), queue_storage_adapter}
  end

  @impl true
  def handle_call(:reset, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.reset_queue(), queue_storage_adapter}
  end
end
