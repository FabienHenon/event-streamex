defmodule EventStreamex.Operators.Executor do
  @moduledoc """
  Executes the operator tasks when an event is received.

  A new executor is started by the `EventStreamex.Operators.Scheduler` as soon
  as an event is received.
  It is responsible to start the operator the scheduler gave it to handle the
  new event.
  And it will handle the operator's failures.

  Meaning that if the operator crashes or returns an error, the executor will
  restart until the operator successfully finishes its task or has failed to many times.

  ## Restart strategy

  In the configuration you can set the restart strategy:

  ```elixir
  config :event_streamex,
    operator_queue_backoff_multiplicator: 2,
    operator_queue_max_restart_time: 10000,
    operator_queue_max_retries: 5,
    operator_queue_min_restart_time: 500
  ```

  * `operator_queue_backoff_multiplicator`: At each new retry, the last wait time is multiplied by this multiplicator to get the new time (ie: If it waited 1.5 second for the last retry and the multiplicator is 2, then, it will now wait 3 seconds before the next restart).
  * `operator_queue_max_restart_time`: The time we wait before a restart is maxed at this value.
  * `operator_queue_max_retries`: The maximum number of times we allow the operators to retry after a failure.
  * `operator_queue_min_restart_time`: The minimum time we will wait before a restart.

  At each failure of an operator, the error logger is called (`EventStreamex.Operators.Logger.ErrorLoggerAdapter`)
  as well as a telemetry.

  When an operator exceeds the maximum number of restarts the executor crashes as
  well as the `EventStreamex.Operators.Scheduler` and no more event will be processed until you
  restart the scheduler with `EventStreamex.restart_scheduler/0`.

  This is because the order of the events is very important and we cannot allow an
  event to fail, otherwise it would compromise your data.

  You can create your own `EventStreamex.Operators.Logger.ErrorLoggerAdapter` and add it to the configuration:

  ```elixir
  config :event_streamex,
    error_logger_adapter: {EventStreamex.Operators.Logger.LoggerAdapter, []}
  ```

  Doing so, you could implement some kind of notification system when there is a
  failure to warn your team about it.

  When a failure happens, maybe it's because of a temporary failure and you could just restart the scheduler.
  But maybe the failure is due to a coding failure.
  In this case you should correct the issue, ship a new version of your app and restart the scheduler (or the entire application).

  With a database queue adapter (the default one), the queue is saved to database and will be loaded again when the
  application restarts.
  You won't miss any event.

  **The executor is used internally, you should never call it directly**

  """
  @moduledoc since: "1.0.0"
  use GenServer
  require Logger
  alias EventStreamex.Operators.Logger.ErrorLoggerAdapter

  @doc false
  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg)
  end

  @doc false
  def start_task(pid) do
    GenServer.cast(pid, :start)
  end

  @doc false
  @impl true
  def init(opts) do
    module = Keyword.get(opts, :module, nil)
    initial_state = Keyword.get(opts, :initial_state, [])

    {:ok,
     %{
       max_retries: Keyword.get(opts, :operator_queue_max_retries, 5),
       max_restart_time: Keyword.get(opts, :operator_queue_max_restart_time, 10000),
       backoff_multiplicator: Keyword.get(opts, :operator_queue_backoff_multiplicator, 1.5),
       monitor_ref: nil,
       module: module,
       job_pid: nil,
       initial_state: initial_state,
       curr_retries: 0,
       curr_time_to_wait: Keyword.get(opts, :operator_queue_min_restart_time, 500),
       start_time: nil
     }}
  end

  defp start_process(module, initial_state) do
    Logger.debug("Starting operator #{inspect(module)}")

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:event_streamex, :process_event, :start],
      %{system_time: System.system_time(), monotonic_time: start_time},
      %{
        type: :process_event,
        module: module,
        event: initial_state
      }
    )

    {:ok, pid} = module.start(initial_state)

    ref = Process.monitor(pid)

    Process.send(pid, :process, [])

    {:ok, pid, ref, start_time}
  end

  @doc false
  @impl true
  def handle_cast(:start, state) do
    {:ok, pid, ref, start_time} = start_process(state.module, state.initial_state)

    Logger.debug("Operator #{inspect(state.module)} started.")

    {:noreply, %{state | job_pid: pid, monitor_ref: ref, start_time: start_time}}
  end

  @doc false
  @impl true
  def handle_info(:restart, state) do
    {:ok, pid, ref, start_time} = start_process(state.module, state.initial_state)

    Logger.debug("Operator #{inspect(state.module)} restarted.")

    {:noreply, %{state | job_pid: pid, monitor_ref: ref, start_time: start_time}}
  end

  @doc false
  @impl true
  def handle_info(
        {:DOWN, ref, :process, _object, :normal},
        %{monitor_ref: ref, start_time: start_time, initial_state: initial_state, module: module} =
          state
      ) do
    Logger.debug("Operator #{inspect(state.module)} job finished.")

    curr_time = System.monotonic_time()

    :telemetry.execute(
      [:event_streamex, :process_event, :stop],
      %{duration: curr_time - start_time, monotonic_time: curr_time},
      %{
        type: :process_event,
        module: module,
        event: initial_state
      }
    )

    {:stop, :normal, state}
  end

  @doc false
  @impl true
  def handle_info(
        {:DOWN, ref, :process, _object, reason},
        %{
          monitor_ref: ref,
          curr_retries: curr_retries,
          max_retries: max_retries,
          start_time: start_time,
          initial_state: initial_state,
          module: module
        } = state
      )
      when curr_retries < max_retries do
    ErrorLoggerAdapter.log_retry(state.module, reason, %{
      max_retries: state.max_retries,
      curr_retries: state.curr_retries,
      max_restart_time: state.max_restart_time,
      backoff_multiplicator: state.backoff_multiplicator,
      curr_time_to_wait: state.curr_time_to_wait
    })

    curr_time = System.monotonic_time()

    :telemetry.execute(
      [:event_streamex, :process_event, :exception],
      %{duration: curr_time - start_time, monotonic_time: curr_time},
      %{
        type: :process_event,
        module: module,
        event: initial_state,
        kind: :error,
        reason: reason,
        stacktrace: Process.info(self(), :current_stacktrace)
      }
    )

    Process.send_after(self(), :restart, state.curr_time_to_wait)

    {:noreply,
     %{
       state
       | curr_retries: state.curr_retries + 1,
         curr_time_to_wait:
           round(
             min(
               state.curr_time_to_wait * state.backoff_multiplicator,
               state.max_restart_time
             )
           )
     }}
  end

  @doc false
  @impl true
  def handle_info(
        {:DOWN, ref, :process, _object, reason},
        %{monitor_ref: ref, start_time: start_time, initial_state: initial_state, module: module} =
          state
      ) do
    ErrorLoggerAdapter.log_failed(state.module, reason, %{
      max_retries: state.max_retries,
      curr_retries: state.curr_retries,
      max_restart_time: state.max_restart_time,
      backoff_multiplicator: state.backoff_multiplicator,
      curr_time_to_wait: state.curr_time_to_wait
    })

    curr_time = System.monotonic_time()

    :telemetry.execute(
      [:event_streamex, :process_event, :exception],
      %{duration: curr_time - start_time, monotonic_time: curr_time},
      %{
        type: :process_event,
        module: module,
        event: initial_state,
        kind: :error,
        reason: reason,
        stacktrace: Process.info(self(), :current_stacktrace)
      }
    )

    {:stop, :job_failed, state}
  end
end
