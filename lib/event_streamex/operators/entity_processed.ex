defmodule EventStreamex.Operators.EntityProcessed do
  @moduledoc """
  This module is called last for each event.

  All it does is register the last updated timestamp of the entity and store it
  in the `EventStreamex.Operators.ProcessStatus` module.

  By default the timestamp is taken from the `updated_at` field of the entity
  but you can change that in the `:process_status_entity_field` config param.

  If no timestamp could be found, the entity is not registered in the process status.

  Works with fields of type:
  * `bigint`: The unix timestamp with a microsecond precision
  * `DateTime`: Using microsecond precision
  * `NaiveDateTime`: Using microsecond precision
  """
  @moduledoc since: "1.1.0"
  require Logger

  def processed(event) do
    [adapter: pubsub_adapter, name: pubsub] = Application.get_env(:event_streamex, :pubsub)

    updated_at_field =
      Application.get_env(:event_streamex, :process_status_entity_field, :updated_at)

    timestamp = Map.get(get_record(event), updated_at_field, nil)
    entity = event.source.table

    res =
      case to_unix_timestamp(timestamp) do
        {:ok, unix_timestamp} ->
          EventStreamex.Operators.ProcessStatus.entity_processed(entity, unix_timestamp)
          unix_timestamp

        {:error, reason} ->
          reason
      end

    Logger.debug(
      "Operator #{inspect(__MODULE__)} terminated successfully with result #{inspect(res)} for entity #{entity}"
    )

    pubsub_adapter.broadcast(
      pubsub,
      "EntityProcessed",
      {"processed", entity, res}
    )

    {:ok, res}
  end

  @doc """
  Transforms the last updated time field value to a unix timestamp.
  The precision must be in microsecond.

  Allowed types are:
  * `bigint`: The unix timestamp with a microsecond precision
  * `DateTime`: Using microsecond precision
  * `NaiveDateTime`: Using microsecond precision

  Returns `{:ok, unix_timestamp}` or `{:error, reason}`

  ## Examples

  ```elixir
  iex> EventStreamex.Operators.EntityProcessed.to_unix_timestamp(nil)
  {:error, :no_value}
  iex> EventStreamex.Operators.EntityProcessed.to_unix_timestamp("bad arg")
  {:error, :bad_arg}
  iex> EventStreamex.Operators.EntityProcessed.to_unix_timestamp(1422057007123000)
  {:ok, 1422057007123000}
  iex> {:ok, datetime, 9000} = DateTime.from_iso8601("2015-01-23T23:50:07.123+02:30")
  iex> EventStreamex.Operators.EntityProcessed.to_unix_timestamp(datetime)
  {:ok, 1422048007123000}
  iex> naivedatetime = NaiveDateTime.from_iso8601!("2015-01-23T23:50:07.123Z")
  iex> EventStreamex.Operators.EntityProcessed.to_unix_timestamp(naivedatetime)
  {:ok, 1422057007123000}
  ```

  """
  @doc since: "1.1.0"
  def to_unix_timestamp(nil), do: {:error, :no_value}

  def to_unix_timestamp(timestamp) when is_integer(timestamp) do
    {:ok, timestamp}
  end

  def to_unix_timestamp(%DateTime{} = datetime) do
    timestamp = datetime |> DateTime.to_unix(:microsecond)

    {:ok, timestamp}
  end

  def to_unix_timestamp(%NaiveDateTime{} = naivedatetime) do
    case DateTime.from_naive(naivedatetime, "Etc/UTC") do
      {:ok, datetime} -> to_unix_timestamp(datetime)
      {:ambiguous, datetime, _datetime2} -> to_unix_timestamp(datetime)
      {:gap, datetime, _datetime2} -> to_unix_timestamp(datetime)
      {:error, reason} -> {:error, reason}
    end
  end

  def to_unix_timestamp(_), do: {:error, :bad_arg}

  @doc """
  Transforms the last updated time field value to a unix timestamp.

  See `to_unix_timestamp/1`
  """
  @doc since: "1.1.0"
  def to_unix_timestamp!(timestamp) do
    {:ok, t} = to_unix_timestamp(timestamp)
    t
  end

  defp get_record(%{type: :delete} = event), do: event.old_record
  defp get_record(event), do: event.new_record
end
