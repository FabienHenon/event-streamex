defmodule EventStreamex.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: EventStreamex.Worker.start_link(arg)
      {WalEx.Supervisor, get_walex_config()}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: EventStreamex.Supervisor]
    Supervisor.start_link(children, opts)
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
    |> IO.inspect(label: "CONF")
  end
end
