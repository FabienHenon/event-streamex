defmodule EventStreamex.Operators.Executor do
  use GenServer
  require Logger
  alias EventStreamex.Operators.Logger.ErrorLoggerAdapter

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    module = Keyword.get(opts, :module, nil)
    initial_state = Keyword.get(opts, :initial_state, [])

    {:ok, pid, ref} = start_process(module, initial_state)

    {:ok,
     %{
       max_retries: Keyword.get(opts, :operator_queue_max_retries, 5),
       max_restart_time: Keyword.get(opts, :operator_queue_max_restart_time, 10000),
       backoff_multiplicator: Keyword.get(opts, :operator_queue_backoff_multiplicator, 1.5),
       monitor_ref: ref,
       module: module,
       job_pid: pid,
       initial_state: initial_state,
       curr_retries: 0,
       curr_time_to_wait: Keyword.get(opts, :operator_queue_min_restart_time, 500)
     }}
  end

  defp start_process(module, initial_state) do
    Logger.debug("Starting operator #{inspect(module)}")

    {:ok, pid} = module.start(initial_state)

    ref = Process.monitor(pid)

    {:ok, pid, ref}
  end

  @impl true
  def handle_info(:restart, state) do
    {:ok, pid, ref} = start_process(state.module, state.initial_state)

    Logger.debug("Operator #{inspect(state.module)} restarted.")

    {:noreply, %{state | job_pid: pid, monitor_ref: ref}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _object, :normal}, %{monitor_ref: ref} = state) do
    Logger.debug("Operator #{inspect(state.module)} job finished.")
    {:stop, :normal, state}
  end

  @impl true
  def handle_info(
        {:DOWN, ref, :process, _object, reason},
        %{monitor_ref: ref, curr_retries: curr_retries, max_retries: max_retries} = state
      )
      when curr_retries < max_retries do
    ErrorLoggerAdapter.log_retry(state.module, reason, %{
      max_retries: state.max_retries,
      curr_retries: state.curr_retries,
      max_restart_time: state.max_restart_time,
      backoff_multiplicator: state.backoff_multiplicator,
      curr_time_to_wait: state.curr_time_to_wait
    })

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

  @impl true
  def handle_info({:DOWN, ref, :process, _object, reason}, %{monitor_ref: ref} = state) do
    ErrorLoggerAdapter.log_failed(state.module, reason, %{
      max_retries: state.max_retries,
      curr_retries: state.curr_retries,
      max_restart_time: state.max_restart_time,
      backoff_multiplicator: state.backoff_multiplicator,
      curr_time_to_wait: state.curr_time_to_wait
    })

    {:stop, :job_failed, state}
  end
end
