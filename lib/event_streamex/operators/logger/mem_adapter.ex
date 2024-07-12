defmodule EventStreamex.Operators.Queue.MemAdapter do
  use GenServer
  @behaviour EventStreamex.Operators.Queue.QueueStorageAdapter

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
end
