defmodule EventStreamex.Operators.ProcessStatus.NoAdapter do
  @moduledoc """
  Do not store the process status.

  This adapter can be used to improve performances.
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
  def item_processed(_item) do
    {:ok, :no_op}
  end

  @doc false
  @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
  def load() do
    {:ok, []}
  end

  @doc false
  @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
  def reset() do
    {:ok, []}
  end

  # Callbacks

  @doc false
  @impl true
  def init(_opts) do
    {:ok, %{}}
  end
end
