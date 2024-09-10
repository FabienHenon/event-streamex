defmodule QueueDbAdapterTest do
  use ExUnit.Case, async: false

  @moduletag :queue

  alias EventStreamex.Operators.Queue.DbAdapter

  setup_all do
    {:ok, pid} = DbAdapter.start_link([])

    on_exit(fn -> Process.exit(pid, :normal) end)

    {:ok, %{}}
  end

  setup do
    DbAdapter.reset_queue()

    on_exit(fn -> DbAdapter.reset_queue() end)

    {:ok, %{}}
  end

  describe "DbAdapter" do
    test "is empty" do
      assert match?({:ok, []}, DbAdapter.load_queue())
    end

    test "has 1 item" do
      uuid1 = UUID.uuid4()
      {:ok, datetime1, _} = DateTime.from_iso8601("2015-01-23T23:50:07.000000Z")

      DbAdapter.add_item(
        {uuid1,
         {%{ModuleName.SubModuleName => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime1,
            lsn: {10, 30}
          }}}
      )

      assert match?(
               {:ok,
                [
                  {^uuid1,
                   {%{ModuleName.SubModuleName => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime1,
                      lsn: {10, 30}
                    }}}
                ]},
               DbAdapter.load_queue()
             )
    end

    test "has 3 items" do
      uuid1 = UUID.uuid4()
      {:ok, datetime1, _} = DateTime.from_iso8601("2015-01-23T23:50:07.000000Z")
      uuid2 = UUID.uuid4()
      {:ok, datetime2, _} = DateTime.from_iso8601("2015-01-23T23:50:08.000000Z")
      uuid3 = UUID.uuid4()
      {:ok, datetime3, _} = DateTime.from_iso8601("2015-01-23T23:50:09.000000Z")

      DbAdapter.add_item(
        {uuid1,
         {%{ModuleName.SubModuleName1 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime1,
            lsn: {10, 30}
          }}}
      )

      DbAdapter.add_item(
        {uuid2,
         {%{ModuleName.SubModuleName2 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime2,
            lsn: {10, 30}
          }}}
      )

      DbAdapter.add_item(
        {uuid3,
         {%{ModuleName.SubModuleName3 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime3,
            lsn: {10, 30}
          }}}
      )

      assert match?(
               {:ok,
                [
                  {^uuid1,
                   {%{ModuleName.SubModuleName1 => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime1,
                      lsn: {10, 30}
                    }}},
                  {^uuid2,
                   {%{ModuleName.SubModuleName2 => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime2,
                      lsn: {10, 30}
                    }}},
                  {^uuid3,
                   {%{ModuleName.SubModuleName3 => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime3,
                      lsn: {10, 30}
                    }}}
                ]},
               DbAdapter.load_queue()
             )
    end

    test "has several processors per item" do
      uuid1 = UUID.uuid4()
      {:ok, datetime1, _} = DateTime.from_iso8601("2015-01-23T23:50:07.000000Z")
      uuid3 = UUID.uuid4()
      {:ok, datetime3, _} = DateTime.from_iso8601("2015-01-23T23:50:09.000000Z")

      DbAdapter.add_item(
        {uuid1,
         {%{ModuleName.SubModuleName1 => false, ModuleName.SubModuleName2 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime1,
            lsn: {10, 30}
          }}}
      )

      DbAdapter.add_item(
        {uuid3,
         {%{ModuleName.SubModuleName3 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime3,
            lsn: {10, 30}
          }}}
      )

      assert match?(
               {:ok,
                [
                  {^uuid1,
                   {%{ModuleName.SubModuleName1 => false, ModuleName.SubModuleName2 => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime1,
                      lsn: {10, 30}
                    }}},
                  {^uuid3,
                   {%{ModuleName.SubModuleName3 => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime3,
                      lsn: {10, 30}
                    }}}
                ]},
               DbAdapter.load_queue()
             )
    end

    test "task completed" do
      uuid1 = UUID.uuid4()
      {:ok, datetime1, _} = DateTime.from_iso8601("2015-01-23T23:50:07.000000Z")
      uuid2 = UUID.uuid4()
      {:ok, datetime2, _} = DateTime.from_iso8601("2015-01-23T23:50:08.000000Z")
      uuid3 = UUID.uuid4()
      {:ok, datetime3, _} = DateTime.from_iso8601("2015-01-23T23:50:09.000000Z")

      DbAdapter.add_item(
        {uuid1,
         {%{ModuleName.SubModuleName1 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime1,
            lsn: {10, 30}
          }}}
      )

      DbAdapter.add_item(
        {uuid2,
         {%{ModuleName.SubModuleName2 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime2,
            lsn: {10, 30}
          }}}
      )

      DbAdapter.add_item(
        {uuid3,
         {%{ModuleName.SubModuleName3 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime3,
            lsn: {10, 30}
          }}}
      )

      DbAdapter.delete_item(
        {uuid1,
         {%{ModuleName.SubModuleName1 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime1,
            lsn: {10, 30}
          }}}
      )

      assert match?(
               {:ok,
                [
                  {^uuid2,
                   {%{ModuleName.SubModuleName2 => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime2,
                      lsn: {10, 30}
                    }}},
                  {^uuid3,
                   {%{ModuleName.SubModuleName3 => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime3,
                      lsn: {10, 30}
                    }}}
                ]},
               DbAdapter.load_queue()
             )
    end

    test "processor completed in task" do
      uuid1 = UUID.uuid4()
      {:ok, datetime1, _} = DateTime.from_iso8601("2015-01-23T23:50:07.000000Z")
      uuid2 = UUID.uuid4()
      {:ok, datetime2, _} = DateTime.from_iso8601("2015-01-23T23:50:08.000000Z")

      DbAdapter.add_item(
        {uuid1,
         {%{
            ModuleName.SubModuleName1 => false,
            ModuleName.SubModuleName2 => false,
            ModuleName.SubModuleName3 => false
          },
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime1,
            lsn: {10, 30}
          }}}
      )

      DbAdapter.add_item(
        {uuid2,
         {%{ModuleName.SubModuleName4 => false},
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime2,
            lsn: {10, 30}
          }}}
      )

      DbAdapter.update_processors_status(
        {uuid1,
         {%{
            ModuleName.SubModuleName1 => false,
            ModuleName.SubModuleName2 => true,
            ModuleName.SubModuleName3 => true
          },
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime1,
            lsn: {10, 30}
          }}}
      )

      assert match?(
               {:ok,
                [
                  {^uuid1,
                   {%{
                      ModuleName.SubModuleName1 => false,
                      ModuleName.SubModuleName2 => true,
                      ModuleName.SubModuleName3 => true
                    },
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime1,
                      lsn: {10, 30}
                    }}},
                  {^uuid2,
                   {%{ModuleName.SubModuleName4 => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime2,
                      lsn: {10, 30}
                    }}}
                ]},
               DbAdapter.load_queue()
             )

      DbAdapter.delete_item(
        {uuid1,
         {%{
            ModuleName.SubModuleName1 => false,
            ModuleName.SubModuleName2 => false,
            ModuleName.SubModuleName3 => false
          },
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
            new_record: %{
              id: "89",
              comment: "Hello",
              post_id: "123"
            },
            old_record: nil,
            changes: nil,
            timestamp: datetime1,
            lsn: {10, 30}
          }}}
      )

      assert match?(
               {:ok,
                [
                  {^uuid2,
                   {%{ModuleName.SubModuleName4 => false},
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
                      new_record: %{
                        id: 89,
                        comment: "Hello",
                        post_id: 123
                      },
                      old_record: nil,
                      changes: nil,
                      timestamp: ^datetime2,
                      lsn: {10, 30}
                    }}}
                ]},
               DbAdapter.load_queue()
             )
    end
  end
end
