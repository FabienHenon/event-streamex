defmodule EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter do
  @moduledoc """
  Behaviour for the process status storage.

  Everytime an event is received we will save its name and timestamp to
  keep a process status up to date.

  The process status allows us to know for each entity the last time it has been processed.
  Here, "processed" means all of these steps:
  - Event received from WAL
  - Event emitted in pubsub
  - Event processed by all operators listening to it

  The adapters keeps a copy of this process status, mostly in case of crash (Except for `EventStreamex.Operators.ProcessStatus.MemAdapter` which keeps the process status only in memory and should not be used in production)

  There are currently 2 process status adapters:
  * `EventStreamex.Operators.ProcessStatus.DbAdapter`: A process status that uses the database tu store its items (This is the default process status adapter)
  * `EventStreamex.Operators.ProcessStatus.MemAdapter`: A process status that stores its items in memory

  You can create your own adapter and set the config to use it.

  Here is the full code of the memory adapter:

  ```elixir
  defmodule EventStreamex.Operators.ProcessStatus.MemAdapter do
    use GenServer
    @behaviour EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter

    @impl true
    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
    def item_processed(item) do
      GenServer.call(__MODULE__, {:save, item})
    end

    @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
    def load() do
      GenServer.call(__MODULE__, :load)
    end

    @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
    def reset() do
      GenServer.call(__MODULE__, :reset)
    end

    # Callbacks

    @impl true
    def init(_opts) do
      {:ok, %{}}
    end

    @impl true
    def handle_call({:save, {entity, timestamp}}, _from, process_status) do
      new_process_status = process_status |> Map.put(entity, timestamp)
      {:reply, {:ok, new_process_status}, new_process_status}
    end

    @impl true
    def handle_call(:load, _from, process_status) do
      {:reply, {:ok, process_status |> Map.to_list()}, process_status}
    end

    @impl true
    def handle_call(:reset, _from, _process_status) do
      {:reply, {:ok, []}, []}
    end
  end
  ```

  The adapter must be a `GenServer` and declare the function `c:start_link/1`.

  Then, it must provide these functions:
  * `c:item_processed/1`: Updates the timestamps of an entity (or adds it if the entity was not known before)
  * `c:load/0`: Return the entire process status
  * `c:reset/0`: Remove all items from the process status

  An item is a tuple containing:
  * The name of the entity (as a string)
  * The timestamp of the last event processed for this entity
  """

  @moduledoc since: "1.1.0"

  @typedoc """
  A process status item.

  An item is a tuple containing:
  * The entity name
  * The timestamp of the last event processed for this entity
  """
  @type process_status() :: {binary(), integer()}

  @typedoc """
  A list of `t:process_status/0`
  """
  @type process_status_list() :: [process_status()]

  @doc """
  Callback called when an entity has just been processed.

  This callback must save it with its timestamps

  The item is a `t:process_status/0`.

  The return value is a result tuple.
  The value in case of success is not used for the moment.
  """
  @doc since: "1.1.0"
  @callback item_processed(process_status()) :: {:ok, term()} | {:error, term()}

  @doc """
  Called at startup or in case of processor crash to return the full list of items.

  The return value is a result tuple with the list of items `t:process_status/0`.
  """
  @doc since: "1.1.0"
  @callback load() :: {:ok, process_status_list()} | {:error, term()}

  @doc """
  Called for tests to reset the process status

  The return value is a result tuple with an empty list.
  """
  @doc since: "1.1.0"
  @callback reset() :: {:ok, process_status_list()} | {:error, term()}

  @doc """
  Called when starting the process status adapter.

  The parameter is the one given in the config file when you set the adapter.

  The return value is a result with the `pid` of the adapter processor.
  """
  @doc since: "1.1.0"
  @callback start_link(any()) :: {:ok, pid()}

  use GenServer
  require Logger

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  def item_processed(item) do
    GenServer.call(__MODULE__, {:save, item})
  end

  @doc false
  def load() do
    GenServer.call(__MODULE__, :load)
  end

  @doc false
  def reset() do
    GenServer.call(__MODULE__, :reset)
  end

  # Callbacks

  @doc false
  @impl true
  def init({process_status_adapter, args}) do
    Logger.debug("ProcessStatusStorageAdapter starting...")
    {:ok, _pid} = process_status_adapter.start_link(args)
    Logger.debug("ProcessStatusStorageAdapter started")
    {:ok, process_status_adapter}
  end

  @doc false
  @impl true
  def handle_call({:save, item}, _from, process_status_adapter) do
    {:reply, process_status_adapter.item_processed(item), process_status_adapter}
  end

  @doc false
  @impl true
  def handle_call(:load, _from, process_status_adapter) do
    {:reply, process_status_adapter.load(), process_status_adapter}
  end

  @doc false
  @impl true
  def handle_call(:reset, _from, process_status_adapter) do
    {:reply, process_status_adapter.reset(), process_status_adapter}
  end
end
