defmodule EventStreamex.Operators.Queue.NoAdapter do
  @moduledoc """
  Adapter that does nothing with the queue.

  This adapter simply improves performances.
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
  def add_item(_item) do
    {:ok, :no_op}
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def delete_item(_item) do
    {:ok, :no_op}
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def update_processors_status(_item) do
    {:ok, :no_op}
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def load_queue() do
    {:ok, []}
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def reset_queue() do
    {:ok, []}
  end

  # Callbacks

  @doc false
  @impl true
  def init(_opts) do
    {:ok, []}
  end
end
