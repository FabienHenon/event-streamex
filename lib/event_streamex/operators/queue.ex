defmodule EventStreamex.Operators.Queue do
  @moduledoc """
  Handle the queue of operators to process.

  Every event received is dispatched to operators (`EventStreamex.Operators.Operator`),
  and executed sequentially to keep ordenancing.

  Every time an event is received, a task is added to the queue of events with each
  operator listening for this event.
  And everytime a an operator task is finished, it is update in the queue, until it is deleted completely when all operators have finished their task.

  A queue item is a tuple containing the operator modules with their completion status and the event to process.

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
  def enqueue([], _event), do: :ok

  def enqueue(modules, event) do
    Agent.update(__MODULE__, fn queue ->
      new_item = {UUID.uuid4(), {modules |> Map.from_keys(false), event}}
      {:ok, _res} = QueueStorageAdapter.add_item(new_item)

      :queue.in(new_item, queue)
    end)
  end

  @doc """
  Tells the queue that the module in the current item has finished.

  It will update its completion state, and if all modules have finished
  the task is completed
  """
  @doc since: "1.2.0"
  def task_finished(module) do
    Agent.update(__MODULE__, fn queue ->
      case :queue.out(queue) do
        {:empty, new_queue} ->
          new_queue

        {{:value, {id, {modules, event}} = item}, new_queue} ->
          new_modules_status =
            modules
            |> Map.put(module, true)

          if Enum.all?(new_modules_status |> Map.values()) do
            {:ok, _res} = QueueStorageAdapter.delete_item(item)
            new_queue
          else
            new_item = {id, {new_modules_status, event}}
            {:ok, _res} = QueueStorageAdapter.update_processors_status(new_item)
            :queue.in_r(new_item, new_queue)
          end
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
