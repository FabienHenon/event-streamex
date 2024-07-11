defmodule EventStreamex.Operators.Scheduler do
  # The scheduler will fail if an operator fails after several restarts
  # The scheduler will have to be manually restarted with
  # EventStreamex.restart_scheduler()
  use GenServer, restart: :temporary

  alias EventStreamex.Operators.{Executor, Queue, Operator}

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @doc """
  Event example:

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
  def process_event(event) do
    GenServer.cast(__MODULE__, {:process_event, event})
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

    {:ok, pid, ref}
  end

  @doc """
  Returns the status of the scheduler.
  """
  def is_alive?() do
    pid = Process.whereis(__MODULE__)
    pid && Process.alive?(pid)
  end

  ## Callbacks

  @impl true
  def init(opts) do
    config = Keyword.get(opts, :config, [])

    {:ok, %{config: config, curr_job: start_operator(Queue.get_task(), config)}}
  end

  @impl true
  def handle_cast({:process_event, event}, %{curr_job: :no_job} = state) do
    modules = get_modules_for_event(event)

    modules
    |> Enum.each(&Queue.enqueue(&1, event))

    {:noreply, %{state | curr_job: start_operator(Queue.get_task(), state.config)}}
  end

  @impl true
  def handle_cast({:process_event, event}, state) do
    event
    |> get_modules_for_event()
    |> Enum.each(&Queue.enqueue(&1, event))

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _object, :normal}, %{curr_job: {:ok, _pid, ref}} = state) do
    Queue.task_finished()

    {:noreply, %{state | curr_job: start_operator(Queue.get_task(), state.config)}}
  end
end