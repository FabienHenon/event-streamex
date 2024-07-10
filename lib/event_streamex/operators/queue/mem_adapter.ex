defmodule EventStreamex.Operators.Queue.MemAdapter do
  use GenServer
  @behaviour EventStreamex.Operators.Queue.QueueStorageAdapter

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def save_queue(queue) do
    GenServer.call(__MODULE__, {:save, queue})
  end

  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def load_queue() do
    GenServer.call(__MODULE__, :load)
  end

  # Callbacks

  @impl true
  def init(_opts) do
    {:ok, []}
  end

  @impl true
  def handle_call({:save, queue}, _from, _state) do
    {:reply, {:ok, queue}, queue}
  end

  @impl true
  def handle_call(:load, _from, queue) do
    {:reply, {:ok, queue}, queue}
  end
end
