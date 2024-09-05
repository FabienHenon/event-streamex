defmodule EventStreamex.Operators.ProcessStatus.MemAdapter do
  @moduledoc """
  Stores the process status in memory.

  This adapter simply keeps the process status in memory.
  This adapter should only be used for tests or dev environments as it will
  not recover the process status in case of a node crash in a distributed system.
  """
  @moduledoc since: "1.0.0"
  use GenServer
  @behaviour EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter

  @doc false
  @impl true
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
  def item_processed(item) do
    GenServer.call(__MODULE__, {:save, item})
  end

  @doc false
  @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
  def load() do
    GenServer.call(__MODULE__, :load)
  end

  @doc false
  @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
  def reset() do
    GenServer.call(__MODULE__, :reset)
  end

  # Callbacks

  @doc false
  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  @doc false
  @impl true
  def handle_call({:save, {entity, timestamp}}, _from, process_status) do
    new_process_status = process_status |> Map.put(entity, timestamp)
    {:reply, {:ok, new_process_status}, new_process_status}
  end

  @doc false
  @impl true
  def handle_call(:load, _from, process_status) do
    {:reply, {:ok, process_status |> Map.to_list()}, process_status}
  end

  @doc false
  @impl true
  def handle_call(:reset, _from, _process_status) do
    {:reply, {:ok, []}, []}
  end
end
