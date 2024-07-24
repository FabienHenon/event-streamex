defmodule EventStreamex.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {EventStreamex.Orchestrator, []}
    ]

    :ok =
      :telemetry.attach_many(
        "streamex-internal-log-handler",
        [
          [:event_streamex, :enqueue_event, :stop],
          [:event_streamex, :enqueue_event, :exception],
          [:event_streamex, :process_event, :stop],
          [:event_streamex, :process_event, :exception]
        ],
        &EventStreamex.Telemetry.EventHandler.handle_event/4,
        nil
      )

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: EventStreamex.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
