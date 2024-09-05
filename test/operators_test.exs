defmodule OperatorsTest do
  use ExUnit.Case, async: false

  setup do
    Utils.LoggerAdapter.set_parent(self())

    on_exit(fn ->
      EventStreamex.Operators.Queue.reset_queue()
      EventStreamex.restart_scheduler()
    end)

    %{}
  end

  describe "RaisedOperator" do
    test "raises on event" do
      pid = Process.whereis(EventStreamex.Operators.Scheduler)
      ref = Process.monitor(pid)

      assert match?(
               :ok,
               EventProtocols.RaisedOperatorEvent.TestOperators.process_insert(
                 %WalEx.Changes.Transaction{
                   changes: [
                     %WalEx.Changes.NewRecord{
                       type: "INSERT",
                       schema: "public",
                       table: "raised_operators",
                       columns: [
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "id",
                           type: "integer",
                           type_modifier: nil
                         },
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "msg",
                           type: "varchar",
                           type_modifier: nil
                         }
                       ],
                       record: %{
                         id: "89",
                         msg: "Hello"
                       },
                       commit_timestamp: 0,
                       lsn: "0/0"
                     }
                   ],
                   commit_timestamp: 0
                 }
               )
             )

      assert_receive {:log_retry, OperatorProtocols.RaisedOperator,
                      {%RuntimeError{message: "Oops"}, _stack},
                      %{
                        max_retries: 1,
                        max_restart_time: 200,
                        backoff_multiplicator: 2,
                        curr_retries: 0,
                        curr_time_to_wait: 100
                      }},
                     1000

      assert_receive {:log_failed, OperatorProtocols.RaisedOperator,
                      {%RuntimeError{message: "Oops"}, _stack},
                      %{
                        max_retries: 1,
                        max_restart_time: 200,
                        backoff_multiplicator: 2,
                        curr_retries: 1,
                        curr_time_to_wait: 200
                      }},
                     1000

      assert_receive {:DOWN, ^ref, :process, ^pid, :job_failed}, 1000

      {:ok, pid} = EventStreamex.restart_scheduler()
      ref = Process.monitor(pid)

      assert_receive {:log_retry, OperatorProtocols.RaisedOperator,
                      {%RuntimeError{message: "Oops"}, _stack},
                      %{
                        max_retries: 1,
                        max_restart_time: 200,
                        backoff_multiplicator: 2,
                        curr_retries: 0,
                        curr_time_to_wait: 100
                      }},
                     1000

      assert_receive {:log_failed, OperatorProtocols.RaisedOperator,
                      {%RuntimeError{message: "Oops"}, _stack},
                      %{
                        max_retries: 1,
                        max_restart_time: 200,
                        backoff_multiplicator: 2,
                        curr_retries: 1,
                        curr_time_to_wait: 200
                      }},
                     1000

      assert_receive {:DOWN, ^ref, :process, ^pid, :job_failed}, 1000
    end
  end

  describe "FailedOperator" do
    test "fails on event" do
      pid = Process.whereis(EventStreamex.Operators.Scheduler)
      ref = Process.monitor(pid)

      assert match?(
               :ok,
               EventProtocols.FailedOperatorEvent.TestOperators.process_insert(
                 %WalEx.Changes.Transaction{
                   changes: [
                     %WalEx.Changes.NewRecord{
                       type: "INSERT",
                       schema: "public",
                       table: "failed_operators",
                       columns: [
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "id",
                           type: "integer",
                           type_modifier: nil
                         },
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "msg",
                           type: "varchar",
                           type_modifier: nil
                         }
                       ],
                       record: %{
                         id: "89",
                         msg: "Hello"
                       },
                       commit_timestamp: 0,
                       lsn: "0/0"
                     }
                   ],
                   commit_timestamp: 0
                 }
               )
             )

      assert_receive {:log_retry, OperatorProtocols.FailedOperator, {:error, :oops},
                      %{
                        max_retries: 1,
                        max_restart_time: 200,
                        backoff_multiplicator: 2,
                        curr_retries: 0,
                        curr_time_to_wait: 100
                      }},
                     1000

      assert_receive {:log_failed, OperatorProtocols.FailedOperator, {:error, :oops},
                      %{
                        max_retries: 1,
                        max_restart_time: 200,
                        backoff_multiplicator: 2,
                        curr_retries: 1,
                        curr_time_to_wait: 200
                      }},
                     1000

      assert_receive {:DOWN, ^ref, :process, ^pid, :job_failed}, 1000

      {:ok, pid} = EventStreamex.restart_scheduler()
      ref = Process.monitor(pid)

      assert_receive {:log_retry, OperatorProtocols.FailedOperator, {:error, :oops},
                      %{
                        max_retries: 1,
                        max_restart_time: 200,
                        backoff_multiplicator: 2,
                        curr_retries: 0,
                        curr_time_to_wait: 100
                      }},
                     1000

      assert_receive {:log_failed, OperatorProtocols.FailedOperator, {:error, :oops},
                      %{
                        max_retries: 1,
                        max_restart_time: 200,
                        backoff_multiplicator: 2,
                        curr_retries: 1,
                        curr_time_to_wait: 200
                      }},
                     1000

      assert_receive {:DOWN, ^ref, :process, ^pid, :job_failed}, 1000
    end
  end

  describe "FilteredOperator" do
    test "when event is not listened" do
      Utils.PubSub.subscribe(:adapter_name, "FilteredOperator")

      assert match?(
               :ok,
               EventProtocols.FilteredOperatorEvent.TestOperators.process_insert(
                 %WalEx.Changes.Transaction{
                   changes: [
                     %WalEx.Changes.NewRecord{
                       type: "INSERT",
                       schema: "public",
                       table: "filtered_operators",
                       columns: [
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "id",
                           type: "integer",
                           type_modifier: nil
                         },
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "msg",
                           type: "varchar",
                           type_modifier: nil
                         }
                       ],
                       record: %{
                         id: "89",
                         msg: "Hello"
                       },
                       commit_timestamp: 0,
                       lsn: "0/0"
                     }
                   ],
                   commit_timestamp: 0
                 }
               )
             )

      assert match?(
               :ok,
               EventProtocols.FilteredOperatorEvent.TestOperators.process_update(
                 %WalEx.Changes.Transaction{
                   changes: [
                     %WalEx.Changes.UpdatedRecord{
                       type: "UPDATE",
                       schema: "public",
                       table: "filtered_operators",
                       columns: [
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "id",
                           type: "integer",
                           type_modifier: nil
                         },
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "msg",
                           type: "varchar",
                           type_modifier: nil
                         }
                       ],
                       record: %{
                         id: "89",
                         msg: "Hello"
                       },
                       old_record: %{
                         id: "90",
                         msg: "Hello you"
                       },
                       commit_timestamp: 0,
                       lsn: "0/0"
                     }
                   ],
                   commit_timestamp: 0
                 }
               )
             )

      assert match?(
               :ok,
               EventProtocols.FilteredOperatorEvent.TestOperators.process_delete(
                 %WalEx.Changes.Transaction{
                   changes: [
                     %WalEx.Changes.DeletedRecord{
                       type: "DELETE",
                       schema: "public",
                       table: "filtered_operators",
                       columns: [
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "id",
                           type: "integer",
                           type_modifier: nil
                         },
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "msg",
                           type: "varchar",
                           type_modifier: nil
                         }
                       ],
                       old_record: %{
                         id: "89",
                         msg: "Hello"
                       },
                       commit_timestamp: 0,
                       lsn: "0/0"
                     }
                   ],
                   commit_timestamp: 0
                 }
               )
             )

      assert_receive {"FilteredOperator", :insert, "filtered_operators"}, 1000
      refute_receive {"FilteredOperator", :update, "filtered_operators"}, 1000
      assert_receive {"FilteredOperator", :delete, "filtered_operators"}, 1000
    end

    @tag :hello
    test "when entity is not listened" do
      Utils.PubSub.subscribe(:adapter_name, "FilteredOperator")

      assert match?(
               :ok,
               EventProtocols.FilteredOperatorEvent.TestOperators.process_insert(
                 %WalEx.Changes.Transaction{
                   changes: [
                     %WalEx.Changes.NewRecord{
                       type: "INSERT",
                       schema: "public",
                       table: "filtered_operators",
                       columns: [
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "id",
                           type: "integer",
                           type_modifier: nil
                         },
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "msg",
                           type: "varchar",
                           type_modifier: nil
                         }
                       ],
                       record: %{
                         id: "89",
                         msg: "Hello"
                       },
                       commit_timestamp: 0,
                       lsn: "0/0"
                     }
                   ],
                   commit_timestamp: 0
                 }
               )
             )

      assert match?(
               :ok,
               EventProtocols.BadOperatorEvent.TestOperators.process_insert(
                 %WalEx.Changes.Transaction{
                   changes: [
                     %WalEx.Changes.NewRecord{
                       type: "INSERT",
                       schema: "public",
                       table: "bad_operators",
                       columns: [
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "id",
                           type: "integer",
                           type_modifier: nil
                         },
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "msg",
                           type: "varchar",
                           type_modifier: nil
                         }
                       ],
                       record: %{
                         id: "89",
                         msg: "Hello"
                       },
                       commit_timestamp: 0,
                       lsn: "0/0"
                     }
                   ],
                   commit_timestamp: 0
                 }
               )
             )

      assert_receive {"FilteredOperator", :insert, "filtered_operators"}, 1000
      refute_receive {"FilteredOperator", :insert, "bad_operators"}, 1000
    end
  end

  describe "LongOperator" do
    test "when several events are enqueued" do
      Utils.PubSub.subscribe(:adapter_name, "FilteredOperator")
      Utils.PubSub.subscribe(:adapter_name, "LongOperator")

      assert match?(
               :ok,
               EventProtocols.LongOperatorEvent.TestOperators.process_insert(
                 %WalEx.Changes.Transaction{
                   changes: [
                     %WalEx.Changes.NewRecord{
                       type: "INSERT",
                       schema: "public",
                       table: "long_operators",
                       columns: [
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "id",
                           type: "integer",
                           type_modifier: nil
                         },
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "msg",
                           type: "varchar",
                           type_modifier: nil
                         }
                       ],
                       record: %{
                         id: "89",
                         msg: "Hello"
                       },
                       commit_timestamp: 0,
                       lsn: "0/0"
                     }
                   ],
                   commit_timestamp: 0
                 }
               )
             )

      assert match?(
               :ok,
               EventProtocols.FilteredOperatorEvent.TestOperators.process_insert(
                 %WalEx.Changes.Transaction{
                   changes: [
                     %WalEx.Changes.NewRecord{
                       type: "INSERT",
                       schema: "public",
                       table: "filtered_operators",
                       columns: [
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "id",
                           type: "integer",
                           type_modifier: nil
                         },
                         %WalEx.Decoder.Messages.Relation.Column{
                           flags: nil,
                           name: "msg",
                           type: "varchar",
                           type_modifier: nil
                         }
                       ],
                       record: %{
                         id: "89",
                         msg: "Hello"
                       },
                       commit_timestamp: 0,
                       lsn: "0/0"
                     }
                   ],
                   commit_timestamp: 0
                 }
               )
             )

      assert_receive {"LongOperator", :pid, pid}, 1000
      refute_receive {"LongOperator", :done}, 1000
      refute_receive {"FilteredOperator", :insert, "filtered_operators"}, 1000

      assert match?(
               [
                 {_uuid1,
                  {OperatorProtocols.LongOperator,
                   %WalEx.Event{
                     name: :long_operators,
                     type: :insert,
                     source: %WalEx.Event.Source{
                       name: "WalEx",
                       version: "4.1.0",
                       db: "postgres",
                       schema: "public",
                       table: "long_operators",
                       columns: %{id: "integer", msg: "varchar"}
                     },
                     new_record: %{id: "89", msg: "Hello"},
                     old_record: nil,
                     changes: nil,
                     timestamp: 0,
                     lsn: "0/0"
                   }}},
                 {_uuid2,
                  {OperatorProtocols.FilteredOperator,
                   %WalEx.Event{
                     name: :filtered_operators,
                     type: :insert,
                     source: %WalEx.Event.Source{
                       name: "WalEx",
                       version: "4.1.0",
                       db: "postgres",
                       schema: "public",
                       table: "filtered_operators",
                       columns: %{id: "integer", msg: "varchar"}
                     },
                     new_record: %{id: "89", msg: "Hello"},
                     old_record: nil,
                     changes: nil,
                     timestamp: 0,
                     lsn: "0/0"
                   }}}
               ],
               EventStreamex.Operators.Queue.get_queue()
             )

      Process.send(pid, :continue, [])

      assert_receive {"LongOperator", :done}, 1000
      assert_receive {"FilteredOperator", :insert, "filtered_operators"}, 1000
    end
  end

  describe "QueryOperator" do
    setup do
      pid = TestDb.get_postgrex()

      {:ok, _res} =
        Postgrex.query(pid, "TRUNCATE base_entities1, base_entities2, merged_entities;", [])

      %{}
    end

    test "merge entities" do
      Utils.PubSub.subscribe(:adapter_name, "base_entities1/dd4bc2ba-c7cc-4a05-a1c7-9f26cd9ab79f")
      Utils.PubSub.subscribe(:adapter_name, "base_entities2/57f6c821-2286-45d5-9896-17946deb6c3e")

      Utils.PubSub.subscribe(
        :adapter_name,
        "merged_entities/57f6c821-2286-45d5-9896-17946deb6c3e"
      )

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities1 (id, base_entity2_id)
               VALUES ('dd4bc2ba-c7cc-4a05-a1c7-9f26cd9ab79f', '57f6c821-2286-45d5-9896-17946deb6c3e');
               """)
             )

      assert_receive {:on_insert, :direct, "base_entities1",
                      %WalEx.Event{
                        name: :base_entities1,
                        type: :insert,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "base_entities1",
                          columns: %{id: "uuid", base_entity2_id: "uuid"}
                        },
                        new_record: %EventProtocols.BaseEntity1OperatorEvent{
                          id: "dd4bc2ba-c7cc-4a05-a1c7-9f26cd9ab79f",
                          base_entity2_id: "57f6c821-2286-45d5-9896-17946deb6c3e"
                        },
                        old_record: nil,
                        changes: nil,
                        timestamp: _timestamp,
                        lsn: _lsn
                      }},
                     1000

      refute_receive {:on_insert, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities2 (id, title)
               VALUES ('57f6c821-2286-45d5-9896-17946deb6c3e', 'My test');
               """)
             )

      assert_receive {:on_insert, :direct, "base_entities2",
                      %WalEx.Event{
                        name: :base_entities2,
                        type: :insert,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "base_entities2",
                          columns: %{id: "uuid", title: "varchar"}
                        },
                        new_record: %EventProtocols.BaseEntity2OperatorEvent{
                          id: "57f6c821-2286-45d5-9896-17946deb6c3e",
                          title: "My test"
                        },
                        old_record: nil,
                        changes: nil,
                        timestamp: _timestamp,
                        lsn: _lsn
                      }},
                     1000

      assert_receive {:on_insert, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :insert,
                        changes: nil,
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "57f6c821-2286-45d5-9896-17946deb6c3e",
                          title: "My test",
                          count: 1
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities1 (id, base_entity2_id)
               VALUES ('5dad2e42-6acc-4954-bf00-cdaf6d8a9802', '57f6c821-2286-45d5-9896-17946deb6c3e');
               """)
             )

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities1 (id, base_entity2_id)
               VALUES ('7391dff8-4f99-49d1-8e5b-ae26b33c5539', 'a847e33f-013b-4810-a491-45150fb598bf');
               """)
             )

      assert_receive {:on_update, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :update,
                        changes: %{count: %{new_value: 2, old_value: 1}},
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "57f6c821-2286-45d5-9896-17946deb6c3e",
                          title: "My test",
                          count: 2
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               DELETE FROM base_entities1 WHERE id = '5dad2e42-6acc-4954-bf00-cdaf6d8a9802';
               """)
             )

      assert_receive {:on_update, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :update,
                        changes: %{count: %{new_value: 1, old_value: 2}},
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "57f6c821-2286-45d5-9896-17946deb6c3e",
                          title: "My test",
                          count: 1
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000
    end

    test "final entity is updated" do
      Utils.PubSub.subscribe(
        :adapter_name,
        "merged_entities/eed1afce-c166-41ba-a0ab-fbb42d9261f4"
      )

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities2 (id, title)
               VALUES ('eed1afce-c166-41ba-a0ab-fbb42d9261f4', 'My test value');
               """)
             )

      assert_receive {:on_insert, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :insert,
                        changes: nil,
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "eed1afce-c166-41ba-a0ab-fbb42d9261f4",
                          title: "My test value",
                          count: 0
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               UPDATE base_entities2 SET title = 'New test value'
               WHERE id = 'eed1afce-c166-41ba-a0ab-fbb42d9261f4';
               """)
             )

      assert_receive {:on_update, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :update,
                        changes: %{
                          title: %{new_value: "New test value", old_value: "My test value"}
                        },
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "eed1afce-c166-41ba-a0ab-fbb42d9261f4",
                          title: "New test value",
                          count: 0
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000
    end

    test "one entity is deleted and cascade to operator entity" do
      Utils.PubSub.subscribe(
        :adapter_name,
        "merged_entities/e186de69-d65c-4a06-8221-1f12003017df"
      )

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities2 (id, title)
               VALUES ('e186de69-d65c-4a06-8221-1f12003017df', 'My test value');
               """)
             )

      assert_receive {:on_insert, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :insert,
                        changes: nil,
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "e186de69-d65c-4a06-8221-1f12003017df",
                          title: "My test value",
                          count: 0
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities1 (id, base_entity2_id)
               VALUES ('6da4b150-75ab-46af-8d9f-f4ca269e0474', 'e186de69-d65c-4a06-8221-1f12003017df');
               """)
             )

      assert_receive {:on_update, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :update,
                        changes: %{
                          count: %{new_value: 1, old_value: 0}
                        },
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "e186de69-d65c-4a06-8221-1f12003017df",
                          title: "My test value",
                          count: 1
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               DELETE FROM base_entities2 WHERE id = 'e186de69-d65c-4a06-8221-1f12003017df';
               """)
             )

      assert_receive {:on_delete, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :delete,
                        changes: nil,
                        lsn: _lsn,
                        old_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "e186de69-d65c-4a06-8221-1f12003017df",
                          title: "My test value",
                          count: 1
                        },
                        new_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000
    end

    test "another entity is deleted but there is no cascade" do
      Utils.PubSub.subscribe(
        :adapter_name,
        "merged_entities/6b9e1f58-bc7d-4474-9716-79d774016f0d"
      )

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities2 (id, title)
               VALUES ('6b9e1f58-bc7d-4474-9716-79d774016f0d', 'My test value');
               """)
             )

      assert_receive {:on_insert, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :insert,
                        changes: nil,
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "6b9e1f58-bc7d-4474-9716-79d774016f0d",
                          title: "My test value",
                          count: 0
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities1 (id, base_entity2_id)
               VALUES ('ba7dfa80-9c72-4f87-ad56-31b01ab2a5d7', '6b9e1f58-bc7d-4474-9716-79d774016f0d');
               """)
             )

      assert_receive {:on_update, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :update,
                        changes: %{
                          count: %{new_value: 1, old_value: 0}
                        },
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "6b9e1f58-bc7d-4474-9716-79d774016f0d",
                          title: "My test value",
                          count: 1
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               DELETE FROM base_entities1 WHERE id = 'ba7dfa80-9c72-4f87-ad56-31b01ab2a5d7';
               """)
             )

      assert_receive {:on_update, :direct, "merged_entities",
                      %WalEx.Event{
                        name: :merged_entities,
                        type: :update,
                        changes: %{count: %{new_value: 0, old_value: 1}},
                        lsn: _lsn,
                        new_record: %EventProtocols.MergedEntityOperatorEvent{
                          id: "6b9e1f58-bc7d-4474-9716-79d774016f0d",
                          title: "My test value",
                          count: 0
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "merged_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000
    end
  end

  describe "ComplexOperator" do
    setup do
      pid = TestDb.get_postgrex()

      {:ok, _res} =
        Postgrex.query(pid, "TRUNCATE base_entities3, base_entities4, complex_entities;", [])

      %{}
    end

    test "multiple queries in operator" do
      Utils.PubSub.subscribe(
        :adapter_name,
        "complex_entities/bc6f95c9-0599-4cdb-89e8-0941cfdfd0b7"
      )

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities4 (id, title)
               VALUES ('bc6f95c9-0599-4cdb-89e8-0941cfdfd0b7', 'My test value');
               """)
             )

      assert_receive {:on_insert, :direct, "complex_entities",
                      %WalEx.Event{
                        name: :complex_entities,
                        type: :insert,
                        changes: nil,
                        lsn: _lsn,
                        new_record: %EventProtocols.ComplexEntityOperatorEvent{
                          id: "bc6f95c9-0599-4cdb-89e8-0941cfdfd0b7",
                          title: "My test value",
                          count: 0
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "complex_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities3 (id, base_entity4_id)
               VALUES ('9db6d30e-e0cb-4ef9-968a-87185d2e4324', 'bc6f95c9-0599-4cdb-89e8-0941cfdfd0b7');
               """)
             )

      assert_receive {:on_update, :direct, "complex_entities",
                      %WalEx.Event{
                        name: :complex_entities,
                        type: :update,
                        changes: %{
                          count: %{new_value: 1, old_value: 0}
                        },
                        lsn: _lsn,
                        new_record: %EventProtocols.ComplexEntityOperatorEvent{
                          id: "bc6f95c9-0599-4cdb-89e8-0941cfdfd0b7",
                          title: "My test value",
                          count: 1
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "complex_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               INSERT INTO base_entities3 (id, base_entity4_id)
               VALUES ('0c7acc67-c78b-46af-9c28-1940061fa04e', 'bc6f95c9-0599-4cdb-89e8-0941cfdfd0b7');
               """)
             )

      assert_receive {:on_update, :direct, "complex_entities",
                      %WalEx.Event{
                        name: :complex_entities,
                        type: :update,
                        changes: %{
                          count: %{new_value: 2, old_value: 1}
                        },
                        lsn: _lsn,
                        new_record: %EventProtocols.ComplexEntityOperatorEvent{
                          id: "bc6f95c9-0599-4cdb-89e8-0941cfdfd0b7",
                          title: "My test value",
                          count: 2
                        },
                        old_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "complex_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000

      assert match?(
               {:ok, %Postgrex.Result{num_rows: 1}},
               Ecto.Adapters.SQL.query(TestRepo, """
               UPDATE base_entities4 SET title = 'Oops' WHERE id = 'bc6f95c9-0599-4cdb-89e8-0941cfdfd0b7';
               """)
             )

      assert_receive {:on_delete, :direct, "complex_entities",
                      %WalEx.Event{
                        name: :complex_entities,
                        type: :delete,
                        changes: nil,
                        lsn: _lsn,
                        old_record: %EventProtocols.ComplexEntityOperatorEvent{
                          id: "bc6f95c9-0599-4cdb-89e8-0941cfdfd0b7",
                          title: "My test value",
                          count: 2
                        },
                        new_record: nil,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "complex_entities",
                          columns: %{count: "int4", id: "uuid", title: "varchar"}
                        },
                        timestamp: _date
                      }},
                     1000
    end
  end
end
