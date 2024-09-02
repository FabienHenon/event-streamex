defmodule EventStreamex.Telemetry.EventHandler do
  @moduledoc false

  @type t :: %{
          type: :enqueue_event | :process_event,
          event: WalEx.Event.t() | nil,
          module: atom()
        }

  require Logger

  def handle_event(
        [:event_streamex, :enqueue_event, :stop],
        measurements,
        metadata,
        _config
      ) do
    Logger.debug(
      "#{to_string(metadata.type)} on #{to_string(metadata.event.type)} for item #{to_string(metadata.event.name)} processed in #{inspect(System.convert_time_unit(measurements.duration, :native, :microsecond))} µs"
    )

    Logger.debug("#{to_string(metadata.type)} event #{inspect(metadata.event)}")
  end

  def handle_event(
        [:event_streamex, :enqueue_event, :exception],
        measurements,
        metadata,
        _config
      ) do
    Logger.error(
      "#{to_string(metadata.type)} on #{to_string(metadata.event.type)} for item #{to_string(metadata.event.name)} errored with reason #{inspect(measurements.reason)} (#{inspect(measurements.kind)})"
    )

    Logger.error("#{to_string(metadata.type)} stacktrace #{inspect(measurements.stacktrace)}")
  end

  def handle_event(
        [:event_streamex, :process_event, :stop],
        measurements,
        metadata,
        _config
      ) do
    Logger.debug(
      "#{to_string(metadata.type)} in module #{to_string(metadata.module)} on #{to_string(metadata.event.type)} for item #{to_string(metadata.event.name)} processed in #{inspect(System.convert_time_unit(measurements.duration, :native, :microsecond))} µs"
    )

    Logger.debug("#{to_string(metadata.type)} event #{inspect(metadata.event)}")
  end

  def handle_event(
        [:event_streamex, :process_event, :exception],
        measurements,
        metadata,
        _config
      ) do
    Logger.error(
      "#{to_string(metadata.type)} in module #{to_string(metadata.module)} on #{to_string(metadata.event.type)} for item #{to_string(metadata.event.name)} errored with reason #{inspect(measurements.reason)} (#{inspect(measurements.kind)})"
    )

    Logger.error("#{to_string(metadata.type)} stacktrace #{inspect(measurements.stacktrace)}")
  end
end
