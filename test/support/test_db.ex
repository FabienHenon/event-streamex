defmodule TestDb do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_postgrex() do
    GenServer.call(__MODULE__, :get_pid)
  end

  defp setup_fixtures(pid) do
    {:ok, _res} =
      Postgrex.query(
        pid,
        "DROP TABLE IF EXISTS base_entities1;",
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        "DROP TABLE IF EXISTS base_entities2;",
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        "DROP TABLE IF EXISTS base_entities3;",
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        "DROP TABLE IF EXISTS base_entities4;",
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        "DROP TABLE IF EXISTS merged_entities;",
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        "DROP TABLE IF EXISTS complex_entities;",
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        CREATE TABLE base_entities1 (
          id uuid DEFAULT gen_random_uuid() NOT NULL,
          base_entity2_id uuid NOT NULL
        );
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY base_entities1
          ADD CONSTRAINT base_entities1_pkey PRIMARY KEY (id);
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        CREATE INDEX base_entities2_base_entity1_id_index ON base_entities1 USING btree (base_entity2_id);
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        CREATE TABLE base_entities2 (
          id uuid DEFAULT gen_random_uuid() NOT NULL,
          title character varying(255)
        );
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY base_entities2
          ADD CONSTRAINT base_entities2_pkey PRIMARY KEY (id);
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        CREATE TABLE base_entities3 (
          id uuid DEFAULT gen_random_uuid() NOT NULL,
          base_entity4_id uuid NOT NULL
        );
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY base_entities3
          ADD CONSTRAINT base_entities3_pkey PRIMARY KEY (id);
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        CREATE INDEX base_entities4_base_entity3_id_index ON base_entities3 USING btree (base_entity4_id);
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        CREATE TABLE base_entities4 (
          id uuid DEFAULT gen_random_uuid() NOT NULL,
          title character varying(255)
        );
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY base_entities4
          ADD CONSTRAINT base_entities4_pkey PRIMARY KEY (id);
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        CREATE TABLE merged_entities (
          id uuid DEFAULT gen_random_uuid() NOT NULL,
          title character varying(255),
          count integer DEFAULT 0
        );
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY merged_entities
          ADD CONSTRAINT merged_entities_pkey PRIMARY KEY (id);
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        CREATE TABLE complex_entities (
          id uuid DEFAULT gen_random_uuid() NOT NULL,
          title character varying(255),
          count integer DEFAULT 0
        );
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY complex_entities
          ADD CONSTRAINT complex_entities_pkey PRIMARY KEY (id);
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY base_entities1 REPLICA IDENTITY FULL;
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY base_entities2 REPLICA IDENTITY FULL;
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY base_entities3 REPLICA IDENTITY FULL;
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY base_entities4 REPLICA IDENTITY FULL;
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY merged_entities REPLICA IDENTITY FULL;
        """,
        []
      )

    {:ok, _res} =
      Postgrex.query(
        pid,
        """
        ALTER TABLE ONLY complex_entities REPLICA IDENTITY FULL;
        """,
        []
      )

    :ok
  end

  @impl true
  def init(opts) do
    {:ok, pid} = EventStreamex.Utils.DbSetup.connect_db(opts)

    :ok = setup_fixtures(pid)

    {:ok, pid}
  end

  @impl true
  def handle_call(:get_pid, _from, pid) do
    {:reply, pid, pid}
  end
end
