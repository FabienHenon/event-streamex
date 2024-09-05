defmodule ProcessStatusDbAdapterTest do
  use ExUnit.Case, async: false

  alias EventStreamex.Operators.ProcessStatus.DbAdapter

  setup_all do
    {:ok, pid} = DbAdapter.start_link([])

    on_exit(fn -> Process.exit(pid, :normal) end)

    {:ok, %{}}
  end

  setup do
    DbAdapter.reset()

    on_exit(fn -> DbAdapter.reset() end)

    {:ok, %{}}
  end

  describe "DbAdapter" do
    test "is empty" do
      assert match?({:ok, []}, DbAdapter.load())
    end

    test "has 1 item" do
      datetime1 = 1_422_057_007_123_000

      DbAdapter.item_processed({"comments", datetime1})

      assert match?(
               {:ok,
                [
                  {"comments", ^datetime1}
                ]},
               DbAdapter.load()
             )
    end

    test "has 3 different items" do
      datetime1 = 1_422_057_007_123_000
      datetime2 = 1_422_057_009_123_000
      datetime3 = 1_422_057_011_123_000

      DbAdapter.item_processed({"comments", datetime1})
      DbAdapter.item_processed({"posts", datetime2})
      DbAdapter.item_processed({"users", datetime3})

      assert match?(
               {:ok,
                [
                  {"comments", ^datetime1},
                  {"posts", ^datetime2},
                  {"users", ^datetime3}
                ]},
               DbAdapter.load()
             )
    end

    test "add a similar item" do
      datetime1 = 1_422_057_007_123_000
      datetime2 = 1_422_057_009_123_000
      datetime3 = 1_422_057_011_123_000
      datetime1_1 = datetime1 + 15000

      DbAdapter.item_processed({"comments", datetime1})
      DbAdapter.item_processed({"posts", datetime2})
      DbAdapter.item_processed({"comments", datetime1_1})
      DbAdapter.item_processed({"users", datetime3})

      assert match?(
               {:ok,
                [
                  {"posts", ^datetime2},
                  {"comments", ^datetime1_1},
                  {"users", ^datetime3}
                ]},
               DbAdapter.load()
             )
    end
  end
end
