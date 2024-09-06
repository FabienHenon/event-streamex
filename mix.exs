defmodule EventStreamex.MixProject do
  use Mix.Project

  def project do
    [
      app: :event_streamex,
      description: "Add event streaming in your Elixir application, using PostgreSQL WAL",
      version: "1.0.0",
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),

      # Docs
      name: "EventStreamex",
      source_url: "https://github.com/FabienHenon/event-streamex",
      homepage_url: "https://github.com/FabienHenon/event-streamex",
      docs: [
        # The main page in the docs
        main: "EventStreamex",
        extras: ["README.md"]
      ]
    ]
  end

  defp package() do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/FabienHenon/event-streamex"}
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {EventStreamex.Application, []},
      env: application_env(Mix.env())
    ]
  end

  defp application_env(:test) do
    [
      app_name: :event_streamex,
      database: "postgres",
      durable_slot: false,
      error_logger_adapter: {Utils.LoggerAdapter, nil},
      hostname: "localhost",
      operator_queue_backoff_multiplicator: 2,
      operator_queue_max_restart_time: 200,
      operator_queue_max_retries: 1,
      operator_queue_min_restart_time: 100,
      password: "",
      port: "5432",
      process_status_entity_field: :updated_at,
      process_status_storage_adapter: {EventStreamex.Operators.ProcessStatus.MemAdapter, []},
      publication: "events",
      pubsub: [adapter: Utils.PubSub, name: :adapter_name],
      queue_storage_adapter: {EventStreamex.Operators.Queue.MemAdapter, []},
      slot_name: "postgres_test_slot",
      url: "",
      username: "postgres"
    ]
  end

  defp application_env(_) do
    [
      app_name: nil,
      database: "postgres",
      durable_slot: true,
      error_logger_adapter: {EventStreamex.Operators.Logger.LoggerAdapter, []},
      hostname: "localhost",
      operator_queue_backoff_multiplicator: 2,
      operator_queue_max_restart_time: 10000,
      operator_queue_max_retries: 5,
      operator_queue_min_restart_time: 500,
      password: "postgres",
      port: "5432",
      process_status_entity_field: :updated_at,
      process_status_storage_adapter: {EventStreamex.Operators.ProcessStatus.DbAdapter, []},
      publication: "events",
      pubsub: [adapter: Phoenix.PubSub, name: nil],
      queue_storage_adapter: {EventStreamex.Operators.Queue.DbAdapter, []},
      slot_name: "postgres_slot",
      url: "",
      username: "postgres"
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:uuid, "~> 1.1"},
      {:walex, "~> 4.1.0"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:phoenix_live_view, "~> 0.20.2", only: :test},
      {:bandit, "~> 1.2", only: :test},
      {:phoenix_ecto, "~> 4.4", only: :test},
      {:ecto_sql, "~> 3.10", only: :test}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
