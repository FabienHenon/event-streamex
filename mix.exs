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
        database: "postgres",
        durable_slot: false,
        hostname: "localhost",
        name: nil,
        password: "postgres",
        port: "5432",
        publication: "events",
        pubsub: [adapter: Phoenix.PubSub, name: nil],
        slot_name: "postgres_slot",
        url: "",
        username: "postgres"
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:walex, path: "../walex"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
      # {:walex, "~> 4.0.0"}
    ]
  end
end
