defmodule EventStreamex.Operators.Queue.DbAdapter do
  @moduledoc """
  A database queue adapter.

  The queue is stored in the database.
  It uses the information from the configuration to connect to the database
  and it automatically creates the database to store the queue.
  """
  @moduledoc since: "1.0.0"
  require Logger
  use GenServer
  @behaviour EventStreamex.Operators.Queue.QueueStorageAdapter

  @derive Jason.Encoder
  defstruct processors: nil, event: nil

  @table_name "event_streamex_queue"

  @doc false
  @impl true
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def add_item(item) do
    GenServer.call(__MODULE__, {:save, item})
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def delete_item(item) do
    GenServer.call(__MODULE__, {:delete, item})
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def update_processors_status(item) do
    GenServer.call(__MODULE__, {:update, item})
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def load_queue() do
    GenServer.call(__MODULE__, :load)
  end

  @doc false
  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def reset_queue() do
    GenServer.call(__MODULE__, :reset)
  end

  defp setup_db(%{table_name: table_name}) do
    {:ok, pid} = EventStreamex.Utils.DbSetup.connect_db(Application.get_all_env(:event_streamex))

    {:ok, res} =
      Postgrex.query(
        pid,
        """
        CREATE TABLE IF NOT EXISTS #{table_name} (
          id              UUID NOT NULL PRIMARY KEY,
          processors      JSONB NOT NULL,
          name            VARCHAR(255) NOT NULL,
          type            VARCHAR(255) NOT NULL,
          source_name     VARCHAR(255) NOT NULL,
          source_version  VARCHAR(255) NOT NULL,
          source_db       VARCHAR(255) NOT NULL,
          source_schema   VARCHAR(255) NOT NULL,
          source_table    VARCHAR(255) NOT NULL,
          source_columns  JSONB NOT NULL,
          new_record      JSONB,
          old_record      JSONB,
          changes         JSONB,
          timestamp       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
          lsn             VARCHAR(255) NOT NULL,
          inserted_at     TIMESTAMP WITHOUT TIME ZONE NOT NULL
        )
        """,
        []
      )

    Logger.debug("Table #{table_name} created. #{inspect(res)}")

    {:ok, pid}
  end

  defp encode_lsn({low, high}) do
    "#{Integer.to_string(low, 16)}/#{Integer.to_string(high, 16)}"
  end

  @doc false
  def decode(%{
        "id" => id,
        "processors" => processors,
        "name" => name,
        "type" => type,
        "source_name" => source_name,
        "source_version" => source_version,
        "source_db" => source_db,
        "source_schema" => source_schema,
        "source_table" => source_table,
        "source_columns" => source_columns,
        "new_record" => new_record,
        "old_record" => old_record,
        "changes" => changes,
        "timestamp" => timestamp,
        "lsn" => lsn
      }) do
    columns = decode_struct_keys(source_columns)

    {id |> UUID.binary_to_string!(),
     {decode_processors(processors),
      %WalEx.Event{
        name: String.to_atom(name),
        type: String.to_atom(type),
        source: %WalEx.Event.Source{
          name: source_name,
          version: source_version,
          db: source_db,
          schema: source_schema,
          table: source_table,
          columns: columns
        },
        new_record: new_record |> decode_struct_keys() |> cast_record(columns),
        old_record: old_record |> decode_struct_keys() |> cast_record(columns),
        changes: decode_struct_keys(changes) |> cast_changes(columns),
        timestamp: Timex.to_datetime(timestamp),
        lsn: lsn |> String.split("/") |> decode_lsn()
      }}}
  end

  defp decode_processors(processors) do
    processors
    |> Jason.decode!()
    |> Map.to_list()
    |> Enum.map(fn {module, status} ->
      {String.to_existing_atom(module), status}
    end)
    |> Map.new()
  end

  defp decode_lsn([i1 | [i2 | _res]]),
    do: {Integer.parse(i1, 16) |> elem(0), Integer.parse(i2, 16) |> elem(0)}

  defp decode_struct_keys(nil), do: nil
  defp decode_struct_keys(list) when is_list(list), do: list |> Enum.map(&decode_struct_keys/1)

  defp decode_struct_keys(obj) when is_map(obj),
    do:
      obj
      |> Enum.map(fn {key, value} -> {String.to_atom(key), decode_struct_keys(value)} end)
      |> Map.new()

  defp decode_struct_keys(v), do: v

  defp encode_processors(processors) do
    processors
    |> Map.to_list()
    |> Enum.map(fn {module, status} ->
      {module |> Atom.to_string(), status}
    end)
    |> Map.new()
    |> Jason.encode!()
  end

  defp cast_changes(nil, _columns), do: nil

  defp cast_changes(changes, columns) do
    changes
    |> Enum.map(fn {key, %{new_value: new_value, old_value: old_value}} ->
      {key,
       %{
         new_value: cast_type(new_value, Map.get(columns, key, nil)),
         old_value: cast_type(old_value, Map.get(columns, key, nil))
       }}
    end)
    |> Map.new()
  end

  defp cast_record(nil, _columns), do: nil

  defp cast_record(record, columns) do
    record
    |> Enum.map(fn {key, value} ->
      {key, cast_type(value, Map.get(columns, key, nil))}
    end)
    |> Map.new()
  end

  defp cast_type(record, "timestamp") when is_binary(record) do
    case Timex.parse(record, "{RFC3339}") do
      {:ok, %NaiveDateTime{} = naive_date_time} ->
        %DateTime{} = date_time = Timex.to_datetime(naive_date_time)
        date_time

      {:ok, %DateTime{} = date_time} ->
        date_time

      _ ->
        WalEx.Types.cast_record(record, "timestamp")
    end
  end

  defp cast_type(value, type) do
    WalEx.Types.cast_record(value, type)
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
        {:save, {id, {processors, item}}},
        _from,
        %{table_name: table_name, pid: pid} = state
      ) do
    res =
      Postgrex.query(
        pid,
        """
        INSERT INTO #{table_name}
          (
            id,
            processors,
            name,
            type,
            source_name,
            source_version,
            source_db,
            source_schema,
            source_table,
            source_columns,
            new_record,
            old_record,
            changes,
            timestamp,
            lsn,
            inserted_at
          )
        VALUES
          (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15,
            NOW()
          )
        """,
        [
          id |> UUID.string_to_binary!(),
          processors |> encode_processors(),
          item.name |> Atom.to_string(),
          item.type |> Atom.to_string(),
          item.source.name,
          item.source.version,
          item.source.db,
          item.source.schema,
          item.source.table,
          item.source.columns,
          item.new_record,
          item.old_record,
          item.changes,
          item.timestamp,
          item.lsn |> encode_lsn()
        ]
      )

    Logger.debug("[QUEUE] Added item with result #{inspect(res)}")

    {:reply, res, state}
  end

  @doc false
  @impl true
  def handle_call(
        {:delete, {id, {_processors, _item}}},
        _from,
        %{table_name: table_name, pid: pid} = state
      ) do
    res =
      Postgrex.query(
        pid,
        """
        DELETE FROM #{table_name}
        WHERE id = $1
        """,
        [id |> UUID.string_to_binary!()]
      )

    Logger.debug("[QUEUE] Deleted item with result #{inspect(res)}")

    {:reply, res, state}
  end

  @doc false
  @impl true
  def handle_call(
        {_action, nil},
        _from,
        state
      ) do
    {:reply, {:ok, :ok}, state}
  end

  @doc false
  @impl true
  def handle_call(
        {:update, {id, {processors, _item}}},
        _from,
        %{table_name: table_name, pid: pid} = state
      ) do
    res =
      Postgrex.query(
        pid,
        """
        UPDATE #{table_name} SET processors = $1
        WHERE id = $2
        """,
        [processors |> encode_processors(), id |> UUID.string_to_binary!()]
      )

    Logger.debug("[QUEUE] Deleted item with result #{inspect(res)}")

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
          id,
          processors,
          name,
          type,
          source_name,
          source_version,
          source_db,
          source_schema,
          source_table,
          source_columns,
          new_record,
          old_record,
          changes,
          timestamp,
          lsn
        FROM #{table_name}
        ORDER BY inserted_at ASC
        """,
        []
      )

    case res do
      %Postgrex.Result{rows: _rows, num_rows: 0} ->
        Logger.debug("[QUEUE] Queue retrieved with no queue")

        {:reply, {:ok, []}, state}

      %Postgrex.Result{rows: rows, columns: columns, num_rows: num_rows}
      when num_rows > 0 ->
        queue =
          rows
          |> Enum.map(fn cols ->
            columns
            |> Enum.zip(cols)
            |> Map.new()
            |> decode()
          end)

        Logger.debug("[QUEUE] Queue retrieved with queue #{inspect(queue)}")

        {:reply, {:ok, queue}, state}
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

    Logger.debug("[QUEUE] Queue reseted #{inspect(res)}")

    {:reply, {:ok, []}, state}
  end
end
