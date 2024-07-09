defmodule EventStreamex.Operators.Queue do
  use Agent

  def start_link(initial_value) do
    Agent.start_link(fn -> initial_value end, name: __MODULE__)
  end

  def get_task do
    Agent.get(__MODULE__, &List.first(Enum.reverse(&1)))
  end

  def get_queue do
    Agent.get(__MODULE__, &Enum.reverse(&1))
  end

  def enqueue(module, event) do
    Agent.update(__MODULE__, &[{module, event} | &1])
  end

  def task_finished() do
    Agent.update(__MODULE__, &Enum.drop(&1, -1))
  end
end
