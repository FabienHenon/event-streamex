defmodule EventStreamex do
  @moduledoc """
  The event streaming system supervisor.

  This supervisor is the one started by `EventStreamex.Orchestrator` when
  the current node is the master node.

  This supervisor is responsible for starting the whole system.
  """

  @moduledoc since: "1.0.0"

  require Logger
  use Supervisor

  @doc false
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @impl true
  def init(_opts) do
    EventStreamex.Utils.DbSetup.setup_db(Application.get_all_env(:event_streamex))

    children =
      [
        {EventStreamex.Operators.Logger.ErrorLoggerAdapter,
         Application.get_env(
           :event_streamex,
           :error_logger_adapter,
           {EventStreamex.Operators.Logger.LoggerAdapter, []}
         )},
        {EventStreamex.Operators.Queue.QueueStorageAdapter,
         Application.get_env(
           :event_streamex,
           :queue_storage_adapter,
           {EventStreamex.Operators.Queue.DbAdapter, []}
         )},
        {EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter,
         Application.get_env(
           :event_streamex,
           :process_status_storage_adapter,
           {EventStreamex.Operators.ProcessStatus.DbAdapter, []}
         )},
        {EventStreamex.Operators.Queue, []},
        {EventStreamex.Operators.ProcessStatus, []},
        {EventStreamex.Operators.Scheduler, [config: Application.get_all_env(:event_streamex)]},
        {WalEx.Supervisor, get_walex_config()}
      ]

    Logger.debug("Starting EventStreamex")

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec restart_scheduler() ::
          {:error, any()} | {:ok, :undefined | pid()} | {:ok, :undefined | pid(), any()}
  @doc """
  Restarts the `EventStreamex.Operators.Scheduler`.

  Should only be used when the Scheduler crashes because of an operator
  that fails after several restarts.

  Because this failure is critical, the sheduler will fail and must be restarted
  manually using this function when the error is solved with the operator.

  When the scheduler is restarted, the failed operator will be restarted
  and then, the rest of the events queue will be executed.
  """
  def restart_scheduler() do
    if EventStreamex.Operators.Scheduler.is_alive?() do
      Process.exit(Process.whereis(EventStreamex.Operators.Scheduler), :normal)
    end

    Supervisor.start_child(
      __MODULE__,
      {EventStreamex.Operators.Scheduler, [config: Application.get_all_env(:event_streamex)]}
    )
  end

  defp get_walex_config() do
    {schema_modules, subscriptions} =
      EventStreamex.Events.get_modules_and_subscriptions()

    [
      database: Application.get_env(:event_streamex, :database),
      durable_slot: Application.get_env(:event_streamex, :durable_slot),
      hostname: Application.get_env(:event_streamex, :hostname),
      modules: schema_modules,
      name: Application.get_env(:event_streamex, :app_name),
      password: Application.get_env(:event_streamex, :password),
      port: Application.get_env(:event_streamex, :port),
      publication: Application.get_env(:event_streamex, :publication),
      slot_name: Application.get_env(:event_streamex, :slot_name),
      subscriptions: subscriptions,
      url: Application.get_env(:event_streamex, :url),
      username: Application.get_env(:event_streamex, :username)
    ]
  end
end
