defmodule EventStreamex.Utils.DbSetup do
  @moduledoc false

  require Logger

  def setup_db(opts) do
    {:ok, pid} = connect_db(opts)

    case publication_exists?(pid, opts) do
      true ->
        Logger.debug("Publication #{Keyword.get(opts, :publication, "events")} already exists")
        :ok

      false ->
        Logger.debug(
          "Publication #{Keyword.get(opts, :publication, "events")} does not exist. Creating..."
        )

        {:ok, _res} = create_publication(pid, opts)
        Logger.debug("Publication #{Keyword.get(opts, :publication, "events")} created!")
    end

    {:ok, _res} = close_connection(pid)

    :ok
  end

  defp publication_exists?(pid, opts) do
    {:ok, %Postgrex.Result{num_rows: num_rows}} =
      Postgrex.query(pid, "SELECT * FROM pg_publication WHERE pubname = $1", [
        Keyword.get(opts, :publication, "events")
      ])

    num_rows == 1
  end

  defp create_publication(pid, opts) do
    Postgrex.query(
      pid,
      "CREATE PUBLICATION #{Keyword.get(opts, :publication, "events")} FOR ALL TABLES",
      []
    )
  end

  defp close_connection(_pid) do
    {:ok, :ok}
  end

  def connect_db(opts) do
    db_configs_from_url =
      opts
      |> Keyword.get(:url, "")
      |> parse_url()

    Postgrex.start_link(
      hostname: Keyword.get(opts, :hostname, db_configs_from_url[:hostname]),
      username: Keyword.get(opts, :username, db_configs_from_url[:username]),
      password: Keyword.get(opts, :password, db_configs_from_url[:password]),
      port: Keyword.get(opts, :port, db_configs_from_url[:port]),
      database: Keyword.get(opts, :database, db_configs_from_url[:database])
    )
  end

  defp parse_url(""), do: []

  defp parse_url(url) when is_binary(url) do
    info = URI.parse(url)

    if is_nil(info.host), do: raise("host is not present in database URL")

    if is_nil(info.path) or not (info.path =~ ~r"^/([^/])+$"),
      do: raise("path should be a database name in database URL")

    destructure [username, password], info.userinfo && String.split(info.userinfo, ":")
    "/" <> database = info.path

    url_opts = set_url_opts(username, password, database, info)

    for {k, v} <- url_opts,
        not is_nil(v),
        do: {k, if(is_binary(v), do: URI.decode(v), else: v)}
  end

  defp set_url_opts(username, password, database, info) do
    url_opts = [
      username: username,
      password: password,
      database: database,
      port: info.port
    ]

    put_hostname_if_present(url_opts, info.host)
  end

  defp put_hostname_if_present(keyword, ""), do: keyword

  defp put_hostname_if_present(keyword, hostname) when is_binary(hostname) do
    Keyword.put(keyword, :hostname, hostname)
  end
end
