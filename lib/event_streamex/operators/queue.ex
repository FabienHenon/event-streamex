defmodule EventStreamex.Operators.Queue do
  @moduledoc """
  Handle the queue of operators to process.

  Every event received is dispatched to operators (`EventStreamex.Operators.Operator`),
  and executed sequentially to keep ordenancing.

  Every time an event is received, a task is added to the queue of events for each
  operator listening for this event.
  And everytime a an operator task is finished, it is removed from the queue.

  A queue item is a tuple containing the operator module and the event to process.

  The queue is handled internally.
  You should not use it directly.
  """
  @moduledoc since: "1.0.0"
  use Agent
  alias EventStreamex.Operators.Queue.QueueStorageAdapter

  require Logger

  @doc false
  def start_link(_initial_value) do
    Logger.debug("Queue starting...")

    res =
      Agent.start_link(
        fn ->
          {:ok, queue} = QueueStorageAdapter.load_queue()
          :queue.from_list(queue)
        end,
        name: __MODULE__
      )

    Logger.debug("Queue started...")

    res
  end

  @doc """
  Retrieves the current task
  """
  @doc since: "1.0.0"
  def get_task do
    Agent.get(__MODULE__, fn queue ->
      case :queue.peek(queue) do
        :empty -> nil
        {:value, item} -> item
      end
      |> get_value()
    end)
  end

  @doc """
  Retrieves the full queue
  """
  @doc since: "1.0.0"
  def get_queue do
    Agent.get(__MODULE__, &:queue.to_list(&1))
  end

  @doc """
  Adds another item to the queue
  """
  @doc since: "1.0.0"
  def enqueue(module, event) do
    Agent.update(__MODULE__, fn queue ->
      new_item = {UUID.uuid4(), {module, event}}
      {:ok, _res} = QueueStorageAdapter.add_item(new_item)

      :queue.in(new_item, queue)
    end)
  end

  @doc """
  Tells the queue that the current item is finished.
  """
  @doc since: "1.0.0"
  def task_finished() do
    Agent.update(__MODULE__, fn queue ->
      case :queue.out(queue) do
        {:empty, new_queue} ->
          new_queue

        {{:value, item}, new_queue} ->
          {:ok, _res} = QueueStorageAdapter.delete_item(item)
          new_queue
      end
    end)
  end

  defp get_value(nil), do: nil
  defp get_value({_id, item}), do: item

  @doc """
  Removes all items from the queue.

  **Should only be used for testing purposes**
  """
  @doc since: "1.0.0"
  def reset_queue() do
    Agent.update(__MODULE__, fn _state ->
      QueueStorageAdapter.reset_queue()
      :queue.new()
    end)
  end
end
