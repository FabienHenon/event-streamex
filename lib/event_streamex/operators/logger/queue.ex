defmodule EventStreamex.Operators.Queue do
  use Agent
  alias EventStreamex.Operators.Queue.QueueStorageAdapter

  require Logger

  def start_link(_initial_value) do
    Logger.debug("Queue starting...")

    res =
      Agent.start_link(
        fn ->
          {:ok, queue} = QueueStorageAdapter.load_queue()
          queue
        end,
        name: __MODULE__
      )

    Logger.debug("Queue started...")

    res
  end

  def get_task do
    Agent.get(__MODULE__, &(List.first(&1) |> get_value()))
  end

  def get_queue do
    Agent.get(__MODULE__, & &1)
  end

  def enqueue(module, event) do
    Agent.update(__MODULE__, fn queue ->
      new_item = {UUID.uuid4(), {module, event}}
      {:ok, _res} = QueueStorageAdapter.add_item(new_item)
      queue ++ [new_item]
    end)
  end

  def task_finished() do
    Agent.update(__MODULE__, fn queue ->
      {item, new_queue} = Enum.split(queue, 1)
      {:ok, _res} = QueueStorageAdapter.delete_item(item |> List.first())
      new_queue
    end)
  end

  defp get_value(nil), do: nil
  defp get_value({_id, item}), do: item

  def reset_queue() do
    Agent.update(__MODULE__, fn _state ->
      QueueStorageAdapter.reset_queue()
      []
    end)
  end
end
