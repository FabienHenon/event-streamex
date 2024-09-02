defmodule EventStreamex.Operators.Logger.LoggerAdapter do
  @moduledoc """
  An error logger adapter that writes logs in the console.

  See `EventStreamex.Operators.Logger.ErrorLoggerAdapter` for more information
  about the error logging.
  """
  @moduledoc since: "1.0.0"
  use GenServer
  @behaviour EventStreamex.Operators.Logger.ErrorLoggerAdapter
  require Logger

  @doc """
  Starts the adapter
  """
  @doc since: "1.0.0"
  @impl EventStreamex.Operators.Logger.ErrorLoggerAdapter
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Logs an error with the `:error` level.
  The operator will be restarted.
  """
  @doc since: "1.0.0"
  @impl EventStreamex.Operators.Logger.ErrorLoggerAdapter
  def log_retry(module, reason, retry_status) do
    GenServer.cast(__MODULE__, {:log_retry, module, reason, retry_status})
  end

  @doc """
  Logs an error with the `:critical` level.
  The operator will not be restarted again.
  """
  @doc since: "1.0.0"
  @impl EventStreamex.Operators.Logger.ErrorLoggerAdapter
  def log_failed(module, reason, retry_status) do
    GenServer.cast(__MODULE__, {:log_failed, module, reason, retry_status})
  end

  # Callbacks

  @doc false
  @impl true
  def init(_opts) do
    {:ok, []}
  end

  @doc false
  @impl true
  def handle_cast({:log_retry, module, reason, retry_status}, state) do
    Logger.error(
      "Operator #{inspect(module)} failed with reason #{inspect(reason)}. Restarting for #{inspect(retry_status.curr_retries + 1)}/#{inspect(retry_status.max_retries)} times, in #{inspect(retry_status.curr_time_to_wait)} ms..."
    )

    {:noreply, state}
  end

  @doc false
  @impl true
  def handle_cast({:log_failed, module, reason, _retry_status}, state) do
    Logger.critical(
      "Operator #{inspect(module)} failed with reason #{inspect(reason)}. Restarted too many times, terminating..."
    )

    {:noreply, state}
  end
end
