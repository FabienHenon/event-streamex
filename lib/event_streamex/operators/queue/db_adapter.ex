defmodule EventStreamex.Operators.Queue.DbAdapter do
  require Logger
  use GenServer
  @behaviour EventStreamex.Operators.Queue.QueueStorageAdapter

  @derive Jason.Encoder
  defstruct module: nil, event: nil

  @table_name "event_streamex_queue"

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def save_queue(queue) do
    GenServer.call(__MODULE__, {:save, queue})
  end

  @impl EventStreamex.Operators.Queue.QueueStorageAdapter
  def load_queue() do
    GenServer.call(__MODULE__, :load)
  end

  defp setup_db(%{table_name: table_name}) do
    {:ok, pid} = EventStreamex.Utils.DbSetup.connect_db(Application.get_all_env(:event_streamex))

    {:ok, res} =
      Postgrex.query(
        pid,
        """
        CREATE TABLE IF NOT EXISTS #{table_name} (
          id integer NOT NULL PRIMARY KEY,
          queue jsonb
        )
        """,
        []
      )

    Logger.debug("Table #{table_name} created. #{inspect(res)}")

    {:ok, pid}
  end

  defp encode(queue) when is_list(queue) do
    queue |> Enum.map(&encode/1)
  end

  defp encode({module, event}) do
    %__MODULE__{
      module: module,
      event:
        Map.update(event, :lsn, nil, fn {i1, i2} ->
          "#{Integer.to_string(i1, 16)}/#{Integer.to_string(i2, 16)}"
        end)
    }
  end

  def decode(queue) when is_list(queue) do
    queue |> Enum.map(&decode/1)
  end

  def decode(%{
        "module" => module,
        "event" => %{
          "name" => name,
          "type" => type,
          "source" => source,
          "new_record" => new_record,
          "old_record" => old_record,
          "changes" => changes,
          "timestamp" => timestamp,
          "lsn" => lsn
        }
      }) do
    new_source = decode_source(source)

    {String.to_existing_atom(module),
     %WalEx.Event{
       name: String.to_atom(name),
       type: String.to_atom(type),
       source: new_source,
       new_record: new_record |> decode_struct_keys() |> cast_record(new_source.columns),
       old_record: old_record |> decode_struct_keys() |> cast_record(new_source.columns),
       changes: decode_struct_keys(changes) |> cast_changes(new_source.columns),
       timestamp: DateTime.from_iso8601(timestamp) |> elem(1),
       lsn: lsn |> String.split("/") |> decode_lsn()
     }}
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

  defp decode_source(nil), do: nil

  defp decode_source(%{
         "version" => version,
         "db" => db,
         "schema" => schema,
         "table" => table,
         "columns" => columns,
         "name" => name
       }) do
    %WalEx.Event.Source{
      name: name,
      version: version,
      db: db,
      schema: schema,
      table: table,
      columns: decode_struct_keys(columns)
    }
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

  @impl true
  def init(opts) do
    table_name = Keyword.get(opts, :table_name, @table_name)

    {:ok, pid} = setup_db(%{table_name: table_name})

    {:ok, %{table_name: table_name, pid: pid}}
  end

  @impl true
  def handle_call(
        {:save, queue},
        _from,
        %{table_name: table_name, pid: pid} = state
      ) do
    res =
      Postgrex.query(
        pid,
        """
        INSERT INTO #{table_name} (id, queue)
         VALUES (1, $1)
        ON CONFLICT(id)
        DO UPDATE SET
         queue = EXCLUDED.queue
        """,
        [queue |> encode()]
      )

    Logger.debug("Saved queue with result #{inspect(res)}")

    {:reply, res, state}
  end

  @impl true
  def handle_call(
        :load,
        _from,
        %{table_name: table_name, pid: pid} = state
      ) do
    # %Postgrex.Result{command: :select, columns: ["queue"], rows: [], num_rows: 0, connection_id: 50207, messages: []}
    {:ok, res} =
      Postgrex.query(
        pid,
        """
        SELECT queue
        FROM #{table_name}
        LIMIT 1
        """,
        []
      )

    case res do
      %Postgrex.Result{rows: _rows, num_rows: 0} ->
        Logger.debug("Queue retrieved with no queue")

        {:reply, {:ok, []}, state}

      %Postgrex.Result{rows: [[raw_queue | _cols] | _rows], num_rows: num_rows}
      when num_rows > 0 ->
        queue = decode(raw_queue)
        Logger.debug("Queue retrieved with queue #{inspect(queue)}")

        {:reply, {:ok, queue}, state}
    end
  end
end
