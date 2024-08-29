defmodule EventStreamex.Operators.Logger.ErrorLoggerAdapter do
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
  Logs an error with the operator, that will be retried.

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

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def log_retry(module, reason, retry_status) do
    GenServer.cast(__MODULE__, {:log_retry, module, reason, retry_status})
  end

  def log_failed(module, reason, retry_status) do
    GenServer.cast(__MODULE__, {:log_failed, module, reason, retry_status})
  end

  # Callbacks

  @impl true
  @spec init({atom(), any()}) :: {:ok, atom()}
  def init({error_logger_adapter, args}) do
    Logger.debug("ErrorLoggerAdapter starting...")
    {:ok, _pid} = error_logger_adapter.start_link(args)
    Logger.debug("ErrorLoggerAdapter started")
    {:ok, error_logger_adapter}
  end

  @impl true
  def handle_cast({:log_retry, module, reason, retry_status}, error_logger_adapter) do
    error_logger_adapter.log_retry(module, reason, retry_status)
    {:noreply, error_logger_adapter}
  end

  @impl true
  def handle_cast({:log_failed, module, reason, retry_status}, error_logger_adapter) do
    error_logger_adapter.log_failed(module, reason, retry_status)
    {:noreply, error_logger_adapter}
  end
end
