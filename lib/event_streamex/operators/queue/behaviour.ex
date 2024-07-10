defmodule EventStreamex.Operators.Queue.QueueStorageAdapter do
  @type queue() :: any()

  @callback save_queue(queue()) :: {:ok, term()} | {:error, term()}

  @callback load_queue() :: {:ok, queue()} | {:error, term()}

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def save_queue(queue) do
    GenServer.call(__MODULE__, {:save, queue})
  end

  def load_queue() do
    GenServer.call(__MODULE__, :load)
  end

  # Callbacks

  @impl true
  def init({queue_storage_adapter, args}) do
    {:ok, _pid} = queue_storage_adapter.start_link(args)
    {:ok, queue_storage_adapter}
  end

  @impl true
  def handle_call({:save, queue}, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.save_queue(queue), queue_storage_adapter}
  end

  @impl true
  def handle_call(:load, _from, queue_storage_adapter) do
    {:reply, queue_storage_adapter.load_queue(), queue_storage_adapter}
  end
end
