defmodule EventStreamex.Operators.Scheduler do
  @moduledoc """
  Is responsible for starting operators as soon as an event is received.

  When an event is received, the scheduler processes it by searching for
  any `EventStreamex.Operators.Operator` listening to it.

  The it will start an `EventStreamex.Operators.Executor` who will be responsible
  for the completion of the operator.

  If the operator fails too many times, then, the executor will also fail,
  as well as the scheduler.

  If this happens that means that no more event will be processed.
  This is a security to avoid inconsistency in your data, as events must be
  processed in order.

  If the scheduler fails, you will have to restart it yourself using `EventStreamex.restart_scheduler()`.

  But before you do that, ensure that the operator will not crash again. Maybe it will
  require some code changes. Or a system to be up and running again.

  Do not hesitate to implement your own `EventStreamex.Operators.Logger.ErrorLoggerAdapter`
  to be notified why a crash appeared.
  """
  @moduledoc since: "1.0.0"
  use GenServer, restart: :temporary

  require Logger

  alias EventStreamex.Operators.{Executor, Queue, Operator}

  @doc false
  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @doc """
  Function used to process an event.

  **You must not use this function yourself**

  ## Event example:

  ```
  %Walex.Event{
    name: :user,
    type: :update,
    source: %WalEx.Event.Source{
      name: "WalEx",
      version: "3.8.0",
      db: "todos",
      schema: "public",
      table: "user",
      columns: %{
        id: "integer",
        name: "varchar",
        created_at: "timestamptz"
      }
    },
    new_record: %{
      id: 1234,
      name: "Chase Pursley",
      created_at: #DateTime<2023-08-18 14:09:05.988369-04:00 -04 Etc/UTC-4>
    },
    # we don't show old_record for update to reduce payload size
    # however, you can see any old values that changed under "changes"
    old_record: nil,
    changes: %{
      name: %{
        new_value: "Chase Pursley",
        old_value: "Chase"
      }
    },
    timestamp: ~U[2023-12-18 15:50:08.329504Z]
  }
  ```
  """
  @doc since: "1.0.0"
  def process_event(pid, event) do
    GenServer.cast(pid, {:process_event, event})
  end

  defp get_modules_for_event(event) do
    event_mapper = %{update: :on_update, insert: :on_insert, delete: :on_delete}
    Operator.get_modules_for_event(Map.get(event_mapper, event.type, nil), event.source.table)
  end

  defp start_operator(nil, _config), do: :no_job

  defp start_operator({module, event}, config) do
    {:ok, pid} =
      Executor.start_link(Keyword.merge([module: module, initial_state: event], config))

    ref = Process.monitor(pid)

    Executor.start_task(pid)

    {:ok, pid, ref}
  end

  @doc """
  Returns the status of the scheduler.
  """
  @doc since: "1.0.0"
  def is_alive?() do
    pid = Process.whereis(__MODULE__)
    not is_nil(pid) && Process.alive?(pid)
  end

  ## Callbacks

  @doc false
  @impl true
  def init(opts) do
    Logger.debug("Scheduler starting...")
    config = Keyword.get(opts, :config, [])

    curr_job = start_operator(Queue.get_task(), config)

    Logger.debug("Scheduler started")

    {:ok,
     %{
       config: config,
       curr_job: curr_job
     }}
  end

  @doc false
  @impl true
  def handle_cast({:process_event, event}, %{curr_job: :no_job} = state) do
    modules = get_modules_for_event(event)

    modules
    |> Enum.each(&Queue.enqueue(&1, event))

    {:noreply, %{state | curr_job: start_operator(Queue.get_task(), state.config)}}
  end

  @doc false
  @impl true
  def handle_cast({:process_event, event}, state) do
    event
    |> get_modules_for_event()
    |> Enum.each(&Queue.enqueue(&1, event))

    {:noreply, state}
  end

  @doc false
  @impl true
  def handle_info({:DOWN, ref, :process, _object, :normal}, %{curr_job: {:ok, _pid, ref}} = state) do
    Queue.task_finished()

    {:noreply, %{state | curr_job: start_operator(Queue.get_task(), state.config)}}
  end
end
