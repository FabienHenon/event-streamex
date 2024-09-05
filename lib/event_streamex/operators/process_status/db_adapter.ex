defmodule EventStreamex.Operators.ProcessStatus.DbAdapter do
  @moduledoc """
  A database queue adapter.

  The queue is stored in the database.
  It uses the information from the configuration to connect to the database
  and it automatically creates the database to store the queue.
  """
  @moduledoc since: "1.0.0"
  require Logger
  use GenServer
  @behaviour EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter

  @derive Jason.Encoder
  defstruct module: nil, event: nil

  @table_name "event_streamex_process_status"

  @doc false
  @impl true
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
  def item_processed(item) do
    GenServer.call(__MODULE__, {:save, item})
  end

  @doc false
  @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
  def load() do
    GenServer.call(__MODULE__, :load)
  end

  @doc false
  @impl EventStreamex.Operators.ProcessStatus.ProcessStatusStorageAdapter
  def reset() do
    GenServer.call(__MODULE__, :reset)
  end

  defp setup_db(%{table_name: table_name}) do
    {:ok, pid} = EventStreamex.Utils.DbSetup.connect_db(Application.get_all_env(:event_streamex))

    {:ok, res} =
      Postgrex.query(
        pid,
        """
        CREATE TABLE IF NOT EXISTS #{table_name} (
          entity          VARCHAR(255) NOT NULL PRIMARY KEY,
          timestamp       BIGINT NOT NULL,
          updated_at      TIMESTAMP WITHOUT TIME ZONE NOT NULL
        )
        """,
        []
      )

    Logger.debug("Table #{table_name} created. #{inspect(res)}")

    {:ok, pid}
  end

  @doc false
  def decode(%{
        "entity" => entity,
        "timestamp" => timestamp
      }) do
    {entity, timestamp}
  end

  # Callbacks

  @doc false
  @impl true
  def init(opts) do
    table_name = Keyword.get(opts, :table_name, @table_name)

    {:ok, pid} = setup_db(%{table_name: table_name})

    {:ok, %{table_name: table_name, pid: pid}}
  end

  @doc false
  @impl true
  def handle_call(
        {:save, {entity, timestamp}},
        _from,
        %{table_name: table_name, pid: pid} = state
      ) do
    res =
      Postgrex.query(
        pid,
        """
        INSERT INTO #{table_name}
          (
            entity,
            timestamp,
            updated_at
          )
        VALUES
          (
            $1,
            $2,
            NOW()
          )
        ON CONFLICT (entity)
        DO UPDATE SET
          timestamp = EXCLUDED.timestamp,
          updated_at = NOW()
        RETURNING *
        """,
        [
          entity,
          timestamp
        ]
      )

    Logger.debug("[PROCESS STATUS] Added item with result #{inspect(res)}")

    {:reply, res, state}
  end

  @doc false
  @impl true
  def handle_call(
        :load,
        _from,
        %{table_name: table_name, pid: pid} = state
      ) do
    {:ok, res} =
      Postgrex.query(
        pid,
        """
        SELECT
          entity,
          timestamp
        FROM #{table_name}
        ORDER BY updated_at
        """,
        []
      )

    case res do
      %Postgrex.Result{rows: _rows, num_rows: 0} ->
        Logger.debug("[PROCESS STATUS] ProcessStatus retrieved with no items")

        {:reply, {:ok, []}, state}

      %Postgrex.Result{rows: rows, columns: columns, num_rows: num_rows}
      when num_rows > 0 ->
        process_status =
          rows
          |> Enum.map(fn cols ->
            columns
            |> Enum.zip(cols)
            |> Map.new()
            |> decode()
          end)

        Logger.debug(
          "[PROCESS STATUS] ProcessStatus retrieved with items #{inspect(process_status)}"
        )

        {:reply, {:ok, process_status}, state}
    end
  end

  @doc false
  @impl true
  def handle_call(
        :reset,
        _from,
        %{table_name: table_name, pid: pid} = state
      ) do
    {:ok, res} =
      Postgrex.query(
        pid,
        """
        DELETE FROM #{table_name}
        """,
        []
      )

    Logger.debug("[PROCESS STATUS] Process status reseted #{inspect(res)}")

    {:reply, {:ok, []}, state}
  end
end
