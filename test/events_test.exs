defmodule EventsTest do
  use ExUnit.Case, async: false

  defmodule TestScheduler do
    use GenServer

    def start_link(parent_process) do
      GenServer.start_link(__MODULE__, parent_process, name: :test_scheduler)
    end

    @impl true
    def init(parent_process) do
      {:ok, parent_process}
    end

    @impl true
    def handle_cast(msg, parent_process) do
      Process.send(parent_process, msg, [])

      {:noreply, parent_process}
    end
  end

  setup do
    {:ok, scheduler_pid} = TestScheduler.start_link(self())

    on_exit(fn ->
      Process.exit(scheduler_pid, :normal)
    end)

    %{}
  end

  describe "ScopedComment" do
    test "on_insert/4", %{} do
      Utils.PubSub.subscribe(:adapter_name, "posts/123/comments")
      Utils.PubSub.subscribe(:adapter_name, "comments")
      Utils.PubSub.subscribe(:adapter_name, "comments/89")

      assert match?(
               :ok,
               EventProtocols.ScopedComment.TestEvents.process_insert(%WalEx.Changes.Transaction{
                 changes: [
                   %WalEx.Changes.NewRecord{
                     type: "INSERT",
                     schema: "public",
                     table: "comments",
                     columns: [
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "id",
                         type: "integer",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "comment",
                         type: "varchar",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "post_id",
                         type: "integer",
                         type_modifier: nil
                       }
                     ],
                     record: %{
                       id: "89",
                       comment: "Hello",
                       post_id: "123"
                     },
                     commit_timestamp: 0,
                     lsn: "0/0"
                   }
                 ],
                 commit_timestamp: 0
               })
             )

      assert_receive {:on_insert, [],
                      %WalEx.Event{
                        name: :comments,
                        type: :insert,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        new_record: %EventProtocols.ScopedComment{
                          id: "89",
                          comment: "Hello",
                          post_id: "123"
                        },
                        old_record: nil,
                        changes: nil,
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000

      assert_receive {:on_insert, :direct,
                      %WalEx.Event{
                        name: :comments,
                        type: :insert,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        new_record: %EventProtocols.ScopedComment{
                          id: "89",
                          comment: "Hello",
                          post_id: "123"
                        },
                        old_record: nil,
                        changes: nil,
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000

      assert_receive {:on_insert, [{"posts", "123"}],
                      %WalEx.Event{
                        name: :comments,
                        type: :insert,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        new_record: %EventProtocols.ScopedComment{
                          id: "89",
                          comment: "Hello",
                          post_id: "123"
                        },
                        old_record: nil,
                        changes: nil,
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000

      assert_receive {:process_event,
                      %WalEx.Event{
                        name: :comments,
                        type: :insert,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        new_record: %{id: "89", comment: "Hello", post_id: "123"},
                        old_record: nil,
                        changes: nil,
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000
    end

    test "on_update/4", %{} do
      Utils.PubSub.subscribe(:adapter_name, "posts/123/comments")
      Utils.PubSub.subscribe(:adapter_name, "comments")
      Utils.PubSub.subscribe(:adapter_name, "comments/89")

      assert match?(
               :ok,
               EventProtocols.ScopedComment.TestEvents.process_update(%WalEx.Changes.Transaction{
                 changes: [
                   %WalEx.Changes.UpdatedRecord{
                     type: "UPDATE",
                     schema: "public",
                     table: "comments",
                     columns: [
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "id",
                         type: "integer",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "comment",
                         type: "varchar",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "post_id",
                         type: "integer",
                         type_modifier: nil
                       }
                     ],
                     record: %{
                       id: "89",
                       comment: "Hello",
                       post_id: "123"
                     },
                     old_record: %{
                       id: "90",
                       comment: "Hello you",
                       post_id: "123"
                     },
                     commit_timestamp: 0,
                     lsn: "0/0"
                   }
                 ],
                 commit_timestamp: 0
               })
             )

      assert_receive {:on_update, [],
                      %WalEx.Event{
                        name: :comments,
                        type: :update,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        new_record: %EventProtocols.ScopedComment{
                          id: "89",
                          comment: "Hello",
                          post_id: "123"
                        },
                        old_record: nil,
                        changes: %{
                          comment: %{
                            new_value: "Hello",
                            old_value: "Hello you"
                          },
                          id: %{
                            new_value: "89",
                            old_value: "90"
                          }
                        },
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000

      assert_receive {:on_update, :direct,
                      %WalEx.Event{
                        name: :comments,
                        type: :update,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        new_record: %EventProtocols.ScopedComment{
                          id: "89",
                          comment: "Hello",
                          post_id: "123"
                        },
                        old_record: nil,
                        changes: %{
                          comment: %{
                            new_value: "Hello",
                            old_value: "Hello you"
                          },
                          id: %{
                            new_value: "89",
                            old_value: "90"
                          }
                        },
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000

      assert_receive {:on_update, [{"posts", "123"}],
                      %WalEx.Event{
                        name: :comments,
                        type: :update,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        new_record: %EventProtocols.ScopedComment{
                          id: "89",
                          comment: "Hello",
                          post_id: "123"
                        },
                        old_record: nil,
                        changes: %{
                          comment: %{
                            new_value: "Hello",
                            old_value: "Hello you"
                          },
                          id: %{
                            new_value: "89",
                            old_value: "90"
                          }
                        },
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000

      assert_receive {:process_event,
                      %WalEx.Event{
                        name: :comments,
                        type: :update,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        new_record: %{id: "89", comment: "Hello", post_id: "123"},
                        old_record: nil,
                        changes: %{
                          comment: %{
                            new_value: "Hello",
                            old_value: "Hello you"
                          },
                          id: %{
                            new_value: "89",
                            old_value: "90"
                          }
                        },
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000
    end

    test "on_delete/4", %{} do
      Utils.PubSub.subscribe(:adapter_name, "posts/123/comments")
      Utils.PubSub.subscribe(:adapter_name, "comments")
      Utils.PubSub.subscribe(:adapter_name, "comments/89")

      assert match?(
               :ok,
               EventProtocols.ScopedComment.TestEvents.process_delete(%WalEx.Changes.Transaction{
                 changes: [
                   %WalEx.Changes.DeletedRecord{
                     type: "DELETE",
                     schema: "public",
                     table: "comments",
                     columns: [
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "id",
                         type: "integer",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "comment",
                         type: "varchar",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "post_id",
                         type: "integer",
                         type_modifier: nil
                       }
                     ],
                     old_record: %{
                       id: "89",
                       comment: "Hello",
                       post_id: "123"
                     },
                     commit_timestamp: 0,
                     lsn: "0/0"
                   }
                 ],
                 commit_timestamp: 0
               })
             )

      assert_receive {:on_delete, [],
                      %WalEx.Event{
                        name: :comments,
                        type: :delete,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        old_record: %EventProtocols.ScopedComment{
                          id: "89",
                          comment: "Hello",
                          post_id: "123"
                        },
                        new_record: nil,
                        changes: nil,
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000

      assert_receive {:on_delete, :direct,
                      %WalEx.Event{
                        name: :comments,
                        type: :delete,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        old_record: %EventProtocols.ScopedComment{
                          id: "89",
                          comment: "Hello",
                          post_id: "123"
                        },
                        new_record: nil,
                        changes: nil,
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000

      assert_receive {:on_delete, [{"posts", "123"}],
                      %WalEx.Event{
                        name: :comments,
                        type: :delete,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        old_record: %EventProtocols.ScopedComment{
                          id: "89",
                          comment: "Hello",
                          post_id: "123"
                        },
                        new_record: nil,
                        changes: nil,
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000

      assert_receive {:process_event,
                      %WalEx.Event{
                        name: :comments,
                        type: :delete,
                        source: %WalEx.Event.Source{
                          name: "WalEx",
                          version: "4.1.0",
                          db: "postgres",
                          schema: "public",
                          table: "comments",
                          columns: %{id: "integer", comment: "varchar", post_id: "integer"}
                        },
                        old_record: %{id: "89", comment: "Hello", post_id: "123"},
                        new_record: nil,
                        changes: nil,
                        timestamp: 0,
                        lsn: "0/0"
                      }},
                     1000
    end

    test "bad event type", %{} do
      Utils.PubSub.subscribe(:adapter_name, "posts/123/comments")
      Utils.PubSub.subscribe(:adapter_name, "comments")
      Utils.PubSub.subscribe(:adapter_name, "comments/89")

      assert match?(
               {:error, :no_events},
               EventProtocols.ScopedComment.TestEvents.process_update(%WalEx.Changes.Transaction{
                 changes: [
                   %WalEx.Changes.DeletedRecord{
                     type: "DELETE",
                     schema: "public",
                     table: "comments",
                     columns: [
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "id",
                         type: "integer",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "comment",
                         type: "varchar",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "post_id",
                         type: "integer",
                         type_modifier: nil
                       }
                     ],
                     old_record: %{
                       id: "89",
                       comment: "Hello",
                       post_id: "123"
                     },
                     commit_timestamp: 0,
                     lsn: "0/0"
                   }
                 ],
                 commit_timestamp: 0
               })
             )

      refute_receive {:on_delete, _, _}, 1000

      refute_receive {:process_event, _}, 1000
    end

    test "bad table", %{} do
      Utils.PubSub.subscribe(:adapter_name, "posts/123/comments")
      Utils.PubSub.subscribe(:adapter_name, "comments")
      Utils.PubSub.subscribe(:adapter_name, "comments/89")

      assert match?(
               {:error, :no_events},
               EventProtocols.ScopedComment.TestEvents.process_delete(%WalEx.Changes.Transaction{
                 changes: [
                   %WalEx.Changes.DeletedRecord{
                     type: "DELETE",
                     schema: "public",
                     table: "bad_table",
                     columns: [
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "id",
                         type: "integer",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "comment",
                         type: "varchar",
                         type_modifier: nil
                       },
                       %WalEx.Decoder.Messages.Relation.Column{
                         flags: nil,
                         name: "post_id",
                         type: "integer",
                         type_modifier: nil
                       }
                     ],
                     old_record: %{
                       id: "89",
                       comment: "Hello",
                       post_id: "123"
                     },
                     commit_timestamp: 0,
                     lsn: "0/0"
                   }
                 ],
                 commit_timestamp: 0
               })
             )

      refute_receive {:on_delete, _, _}, 1000

      refute_receive {:process_event, _}, 1000
    end
  end
end
