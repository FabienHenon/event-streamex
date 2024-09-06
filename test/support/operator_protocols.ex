defmodule OperatorProtocols do
  defmodule RaisedOperator do
    use EventStreamex.Operators.Operator,
      schema: "raised_operators",
      repo: TestRepo

    on_event(
      all_events(),
      entities(["raised_operators"]),
      fn _item ->
        raise "Oops"
      end
    )
  end

  defmodule FailedOperator do
    use EventStreamex.Operators.Operator,
      schema: "failed_operators",
      repo: TestRepo

    on_event(
      all_events(),
      entities(["failed_operators"]),
      fn _item ->
        {:error, :oops}
      end
    )
  end

  defmodule FilteredOperator do
    use EventStreamex.Operators.Operator,
      schema: "filtered_operators",
      repo: TestRepo

    on_event(
      events([:on_insert, :on_delete]),
      entities(["filtered_operators"]),
      fn item ->
        Utils.PubSub.broadcast(
          :adapter_name,
          "FilteredOperator",
          {"FilteredOperator", item.type, item.source.table}
        )

        {:ok, item.type}
      end
    )
  end

  defmodule LongOperator do
    use EventStreamex.Operators.Operator,
      schema: "long_operators",
      repo: TestRepo

    on_event(
      all_events(),
      entities(["long_operators"]),
      fn item ->
        Utils.PubSub.broadcast(
          :adapter_name,
          "LongOperator",
          {"LongOperator", :pid, self()}
        )

        receive do
          :continue ->
            Utils.PubSub.broadcast(
              :adapter_name,
              "LongOperator",
              {"LongOperator", :done}
            )

            {:ok, item.type}
        after
          10000 ->
            {:error, :timeout}
        end
      end
    )
  end

  defmodule OtherLongOperator do
    use EventStreamex.Operators.Operator,
      schema: "other_long_operators",
      repo: TestRepo

    on_event(
      all_events(),
      entities(["long_operators"]),
      fn item ->
        Utils.PubSub.broadcast(
          :adapter_name,
          "LongOperator",
          {"OtherLongOperator", :pid, self()}
        )

        receive do
          :continue ->
            Utils.PubSub.broadcast(
              :adapter_name,
              "LongOperator",
              {"OtherLongOperator", :done}
            )

            {:ok, item.type}
        after
          10000 ->
            {:error, :timeout}
        end
      end
    )
  end

  defmodule MergeOperator do
    use EventStreamex.Operators.Operator,
      schema: "merged_entities",
      repo: TestRepo

    on_event(
      all_events(),
      entities(["base_entities1", "base_entities2"]),
      query(
        """
        MERGE INTO merged_entities dest
        USING (
            SELECT
                p.id AS id,
                p.title AS title,
                (SELECT COUNT(c.id) FROM base_entities1 c WHERE c.base_entity2_id = p.id) AS count
            FROM base_entities2 p
            WHERE p.id = $1
        ) src ON src.id = dest.id
        WHEN NOT MATCHED THEN
          INSERT (id, title, count)
          VALUES (src.id, src.title, src.count)
        WHEN MATCHED THEN
          UPDATE SET
              title = src.title,
              count = src.count;
        """,
        id([{"base_entities1", :base_entity2_id}, {"base_entities2", :id}]),
        delete_with(["base_entities2"])
      )
    )
  end

  defmodule ComplexOperator do
    use EventStreamex.Operators.Operator,
      schema: "complex_entities",
      repo: TestRepo

    on_event(
      all_events(),
      entities(["base_entities3", "base_entities4"]),
      fn
        %{type: :insert, name: :base_entities4} = item ->
          {:ok, res} =
            Ecto.Adapters.SQL.query(
              TestRepo,
              """
              INSERT INTO complex_entities (id, title, count)
              VALUES ($1, $2, 0);
              """,
              [item.new_record.id |> UUID.string_to_binary!(), item.new_record.title]
            )

          {:ok, res}

        %{type: :update, name: :base_entities4} = item ->
          {:ok, res} =
            Ecto.Adapters.SQL.query(
              TestRepo,
              """
              DELETE FROM complex_entities WHERE id = $1;
              """,
              [item.new_record.id |> UUID.string_to_binary!()]
            )

          {:ok, res}

        %{type: :insert, name: :base_entities3} = item ->
          {:ok, %Postgrex.Result{rows: [[count]]}} =
            Ecto.Adapters.SQL.query(
              TestRepo,
              """
              SELECT COUNT(*) AS count FROM base_entities3 WHERE base_entity4_id = $1;
              """,
              [item.new_record.base_entity4_id |> UUID.string_to_binary!()]
            )

          {:ok, res} =
            Ecto.Adapters.SQL.query(
              TestRepo,
              """
              UPDATE complex_entities SET count = $2 WHERE id = $1;
              """,
              [item.new_record.base_entity4_id |> UUID.string_to_binary!(), count]
            )

          {:ok, res}

        _ ->
          {:ok, :ok}
      end
    )
  end
end
