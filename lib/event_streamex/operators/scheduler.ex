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

  @ets_name :scheduler

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
    event
    |> get_modules_for_event()
    |> Queue.enqueue(event)

    case curr_job() do
      [] ->
        GenServer.cast(pid, :process_event)

      :no_scheduler ->
        # The event is enqueued and will be executed when the scheduler will
        # be restarted anyway
        :ok

      _ ->
        :ok
    end

    :ok
  end

  defp get_modules_for_event(event) do
    event_mapper = %{update: :on_update, insert: :on_insert, delete: :on_delete}

    Operator.get_modules_for_event(
      Map.get(event_mapper, event.type, nil),
      event.source.table
    )
  end

  defp update_processed_entity(event) do
    # This will save the processed timestamp for the entity.
    # It is executed last before the timestamp must be saved once all operators
    # have processed the entity
    EventStreamex.Operators.EntityProcessed.processed(event)
  end

  @doc """
  Gets the information of the given operator being executed or `:no_job`.

  The return value is like this: `{pid(), ref(), event(), boolean()}`
  """
  @doc since: "1.2.0"
  def curr_job(module) do
    case :ets.lookup(@ets_name, module) do
      [{^module, curr_job}] ->
        curr_job

      _ ->
        :no_job
    end
  end

  @doc """
  Gets the list of operators being executed with their status.

  The return value is like this: `[{atom(), {pid(), ref(), boolean()}}]`
  """
  @doc since: "1.2.0"
  def curr_job() do
    try do
      :ets.tab2list(@ets_name)
    catch
      t ->
        Logger.warning(
          "ETS table #{@ets_name} does not exist. This may be becuse the scheduler is not launched yet or has crashed (#{inspect(t)})"
        )

        :no_scheduler
    end
  end

  defp start_operator(nil, _config) do
    :ets.delete_all_objects(@ets_name)
    {:ok, :no_job}
  end

  defp start_operator({[], _event}, _config) do
    :ets.delete_all_objects(@ets_name)
    {:ok, :no_job}
  end

  defp start_operator({modules, event}, config) do
    :ets.delete_all_objects(@ets_name)

    {:ok,
     modules
     |> Map.to_list()
     |> Enum.map(fn
       {module, false} ->
         {:ok, pid} =
           Executor.start_link(Keyword.merge([module: module, initial_state: event], config))

         ref = Process.monitor(pid)

         Executor.start_task(pid)

         curr_job = {pid, ref, event, false}

         :ets.insert(@ets_name, {module, curr_job})

         curr_job

       {module, true} ->
         :ets.insert(@ets_name, {module, {nil, nil, event, true}})
     end)}
  end

  defp processor_completed(module) do
    case curr_job(module) do
      :no_job ->
        false

      {pid, ref, event, _status} ->
        :ets.insert(@ets_name, {module, {pid, ref, event, true}})
        true
    end
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

    table = :ets.new(@ets_name, [:set, :protected, :named_table, read_concurrency: true])

    {:ok, _curr_jobs} = start_operator(Queue.get_task(), config)

    Logger.debug("Scheduler started")

    {:ok,
     %{
       config: config,
       ets_table: table
     }}
  end

  @doc false
  @impl true
  def handle_cast(:process_event, state) do
    case curr_job() do
      [] ->
        {:ok, _curr_jobs} = start_operator(Queue.get_task(), state.config)

        {:noreply, state}

      _curr_jos ->
        {:noreply, state}
    end
  end

  @doc false
  @impl true
  def handle_info({:DOWN, ref, :process, _object, :normal}, state) do
    curr_job()
    |> Enum.reduce({true, nil}, fn
      {module, {_pid, ^ref, event, _status}}, {final_status, _e} ->
        Queue.task_finished(module)
        {processor_completed(module) && final_status, event}

      {_module, {_pid, _ref, event, status}}, {final_status, _e} ->
        {status && final_status, event}
    end)
    |> case do
      {true, event} ->
        {:ok, _ref} = update_processed_entity(event)

        {:ok, _curr_jobs} = start_operator(Queue.get_task(), state.config)

      _ ->
        :ok
    end

    {:noreply, state}
  end
end
