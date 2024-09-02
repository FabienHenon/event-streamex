defmodule EventStreamex.Operators.Queue.MemAdapter do
  @moduledoc """
  Stores the queue in memory.

  This adapter simply keeps the queue in memory.
  This adapter should only be used for tests or dev environments as it will
  not recover the queue in case of a node crash in a distributed system.
  """
  @moduledoc since: "1.0.0"
  use GenServer
  @behaviour EventStreamex.Operators.Queue.QueueStorageAdapter

  @doc false
  @impl true
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def add_item(item) do
    GenServer.call(__MODULE__, {:save, item})
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def delete_item(item) do
    GenServer.call(__MODULE__, {:delete, item})
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def load_queue() do
    GenServer.call(__MODULE__, :load)
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def reset_queue() do
    GenServer.call(__MODULE__, :reset)
  end

  # Callbacks

  @doc false
  @impl true
  def init(_opts) do
    {:ok, []}
  end

  @doc false
  @impl true
  def handle_call({:save, item}, _from, queue) do
    new_queue = queue ++ [item]
    {:reply, {:ok, new_queue}, new_queue}
  end

  @doc false
  @impl true
  def handle_call({:delete, {id, _item}}, _from, queue) do
    new_queue = queue |> Enum.reject(fn {i, _t} -> i == id end)
    {:reply, {:ok, new_queue}, new_queue}
  end

  @doc false
  @impl true
  def handle_call({_action, nil}, _from, queue) do
    {:reply, {:ok, queue}, queue}
  end

  @doc false
  @impl true
  def handle_call(:load, _from, queue) do
    {:reply, {:ok, queue}, queue}
  end

  @doc false
  @impl true
  def handle_call(:reset, _from, _queue) do
    {:reply, {:ok, []}, []}
  end
end
