defmodule EventStreamex.Operators.Logger.ErrorLoggerAdapter do
  @moduledoc """
  Behaviour to handle errors with operators (`EventStreamex.Operators.Operator`).

  When implementing this behaviour, you must put use a `GenServer`.

  There is currently one adapter available and used by default: `EventStreamex.Operators.Logger.LoggerAdapter`.
  This adapter simply logs the error to the console.

  The executor (`EventStreamex.Operators.Executor`) who catches the operator failure and
  calls the error logger will also execute a telemetry event so that you can
  see errors over time in a chart.

  There are 2 kinds of errors:
  * **Retry**: The operator failed but will retry soon
  * **Failed**: The operator retried several times but failed again.

  For more information about the retry strategy, see `EventStreamex.Operators.Executor`.
  """
  @moduledoc since: "1.0.0"

  @typedoc """
  The retry status:

  * `max_retries`: Maximum number of retries attempted
  * `curr_retries`: The current retries count already attempted (starting at 1 after the first failure)
  * `max_restart_time`: The maximum number of milliseconds to wait for the next retry
  * `backoff_multiplicator`: A decimal number multiplied to the `curr_time_to_wait` to define the next amount of time to wait for the next retry (maxed by `max_restart_time`)
  * `curr_time_to_wait`: The current time to wait in milliseconds before the next retry
  """
  @type retry_status() ::
          %{
            max_retries: integer(),
            curr_retries: integer(),
            max_restart_time: integer(),
            backoff_multiplicator: float(),
            curr_time_to_wait: integer()
          }

  @doc """
  Logs an error with the operator, that will be retried again soon.

  # Params

  * `module`: The module of the operator
  * `reason`: The failure reason
  * `retry_status`: The current retry count, the time to wait for the next retry, etc...
  """
  @callback log_retry(atom(), any(), retry_status()) :: :ok

  @doc """
  Same as `log_retry/3` but when no more retry will be attempted

  # Params

  * `module`: The module of the operator
  * `reason`: The failure reason
  * `retry_status`: The current retry count, the time to wait for the next retry, etc...
  """
  @callback log_failed(atom(), any(), retry_status()) :: :ok

  @doc """
  This function will be called at startup to spawn the error logger adapter.

  The param comes from the configuration of the adapter:

  ```elixir
  error_logger_adapter: {EventStreamex.Operators.Logger.LoggerAdapter, []},
  ```

  The return value is a tuple with the pid of the spawned process
  """
  @callback start_link(any()) :: {:ok, pid()}

  use GenServer
  require Logger

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  def log_retry(module, reason, retry_status) do
    GenServer.cast(__MODULE__, {:log_retry, module, reason, retry_status})
  end

  @doc false
  def log_failed(module, reason, retry_status) do
    GenServer.cast(__MODULE__, {:log_failed, module, reason, retry_status})
  end

  # Callbacks

  @doc false
  @impl true
  @spec init({atom(), any()}) :: {:ok, atom()}
  def init({error_logger_adapter, args}) do
    Logger.debug("ErrorLoggerAdapter starting...")
    {:ok, _pid} = error_logger_adapter.start_link(args)
    Logger.debug("ErrorLoggerAdapter started")
    {:ok, error_logger_adapter}
  end

  @doc false
  @impl true
  def handle_cast({:log_retry, module, reason, retry_status}, error_logger_adapter) do
    error_logger_adapter.log_retry(module, reason, retry_status)
    {:noreply, error_logger_adapter}
  end

  @doc false
  @impl true
  def handle_cast({:log_failed, module, reason, retry_status}, error_logger_adapter) do
    error_logger_adapter.log_failed(module, reason, retry_status)
    {:noreply, error_logger_adapter}
  end
end
