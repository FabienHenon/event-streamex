defmodule TestRepo do
  use Ecto.Repo,
    otp_app: :event_streamex,
    adapter: Ecto.Adapters.Postgres

  @impl true
  def init(:supervisor, config),
    do:
      {:ok,
       config
       |> Keyword.merge(
         database: Application.get_env(:event_streamex, :database),
         username: Application.get_env(:event_streamex, :username),
         password: Application.get_env(:event_streamex, :password),
         hostname: Application.get_env(:event_streamex, :hostname)
       )}

  @impl true
  def init(:runtime, config),
    do:
      {:ok,
       config
       |> Keyword.merge(
         database: Application.get_env(:event_streamex, :database),
         username: Application.get_env(:event_streamex, :username),
         password: Application.get_env(:event_streamex, :password),
         hostname: Application.get_env(:event_streamex, :hostname)
       )}
end
