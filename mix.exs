defmodule EventStreamex.MixProject do
  use Mix.Project

  def project do
    [
      app: :event_streamex,
      version: "0.1.0",
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {EventStreamex.Application, []},
      env: [
        app_name: nil,
        database: "postgres",
        durable_slot: false,
        error_logger_adapter: {EventStreamex.Operators.Logger.LoggerAdapter, []},
        hostname: "localhost",
        operator_queue_backoff_multiplicator: 2,
        operator_queue_max_restart_time: 10000,
        operator_queue_max_retries: 5,
        operator_queue_min_restart_time: 500,
        password: "postgres",
        port: "5432",
        publication: "events",
        pubsub: [adapter: Phoenix.PubSub, name: nil],
        queue_storage_adapter: {EventStreamex.Operators.Queue.MemAdapter, []},
        slot_name: "postgres_slot",
        url: "",
        username: "postgres"
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:uuid, "~> 1.1"},
      {:walex, path: "../walex"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
      # {:walex, "~> 4.0.0"}
    ]
  end
end
