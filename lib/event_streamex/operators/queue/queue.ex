defmodule EventStreamex.Operators.Queue do
  use Agent
  alias EventStreamex.Operators.Queue.QueueStorageAdapter

  def start_link(_initial_value) do
    Agent.start_link(
      fn ->
        {:ok, queue} = QueueStorageAdapter.load_queue()
        queue
      end,
      name: __MODULE__
    )
  end

  def get_task do
    Agent.get(__MODULE__, &List.first(Enum.reverse(&1)))
  end

  def get_queue do
    Agent.get(__MODULE__, &Enum.reverse(&1))
  end

  def enqueue(module, event) do
    Agent.update(__MODULE__, fn queue ->
      new_queue = [{module, event} | queue]
      {:ok, _res} = QueueStorageAdapter.save_queue(new_queue)
      new_queue
    end)
  end

  def task_finished() do
    Agent.update(__MODULE__, fn queue ->
      new_queue = Enum.drop(queue, -1)
      {:ok, _res} = QueueStorageAdapter.save_queue(new_queue)
      new_queue
    end)
  end
end
