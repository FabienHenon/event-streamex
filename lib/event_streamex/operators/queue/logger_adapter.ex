defmodule EventStreamex.Operators.Logger.LoggerAdapter do
  use GenServer
  @behaviour EventStreamex.Operators.Logger.ErrorLoggerAdapter
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl EventStreamex.Operators.Logger.ErrorLoggerAdapter
  def log_retry(module, reason, retry_status) do
    GenServer.cast(__MODULE__, {:log_retry, module, reason, retry_status})
  end

  @impl EventStreamex.Operators.Logger.ErrorLoggerAdapter
  def log_failed(module, reason, retry_status) do
    GenServer.cast(__MODULE__, {:log_failed, module, reason, retry_status})
  end

  # Callbacks

  @impl true
  def init(_opts) do
    {:ok, []}
  end

  @impl true
  def handle_cast({:log_retry, module, reason, retry_status}, state) do
    Logger.error(
      "Operator #{inspect(module)} failed with reason #{inspect(reason)}. Restarting for #{inspect(retry_status.curr_retries + 1)}/#{inspect(retry_status.max_retries)} times, in #{inspect(retry_status.curr_time_to_wait)} ms..."
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:log_failed, module, reason, _retry_status}, state) do
    Logger.critical(
      "Operator #{inspect(module)} failed with reason #{inspect(reason)}. Restarted too many times, terminating..."
    )

    {:noreply, state}
  end
end
