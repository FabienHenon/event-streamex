defmodule ProcessStatusTest do
  use ExUnit.Case, async: false

  alias EventStreamex.Operators.ProcessStatus

  setup do
    ProcessStatus.reset()

    on_exit(fn -> ProcessStatus.reset() end)

    {:ok, %{}}
  end

  describe "ProcessStatus" do
    test "has no item" do
      assert ProcessStatus.last_processed("comments") == nil
    end

    test "has 1 item" do
      ProcessStatus.entity_processed("comments", 1_422_057_007_123_000)

      assert ProcessStatus.last_processed("comments") == 1_422_057_007_123_000
    end

    test "is processed when same timestamp" do
      ProcessStatus.entity_processed("comments", 1_422_057_007_123_000)

      assert ProcessStatus.processed?("comments", 1_422_057_007_123_000) == true
    end

    test "is processed when later" do
      ProcessStatus.entity_processed("comments", 1_422_057_007_123_000 + 1000)

      assert ProcessStatus.processed?("comments", 1_422_057_007_123_000) == true
    end

    test "is processed when earlier" do
      ProcessStatus.entity_processed("comments", 1_422_057_007_123_000 - 1000)

      assert ProcessStatus.processed?("comments", 1_422_057_007_123_000) == false
    end

    test "has 3 different items" do
      ProcessStatus.entity_processed("comments", 1_422_057_007_123_000)
      ProcessStatus.entity_processed("users", 1_422_057_010_123_000)
      ProcessStatus.entity_processed("posts", 1_422_057_005_123_000)

      assert ProcessStatus.processed?("comments", 1_422_057_007_123_000) == true
      assert ProcessStatus.processed?("users", 1_422_057_007_123_000) == true
      assert ProcessStatus.processed?("posts", 1_422_057_007_123_000) == false
    end

    test "has 3 items with an updated one" do
      ProcessStatus.entity_processed("comments", 1_422_057_007_123_000)
      ProcessStatus.entity_processed("users", 1_422_057_010_123_000)
      ProcessStatus.entity_processed("posts", 1_422_057_005_123_000)

      ProcessStatus.entity_processed("posts", 1_422_057_008_123_000)

      assert ProcessStatus.processed?("comments", 1_422_057_007_123_000) == true
      assert ProcessStatus.processed?("users", 1_422_057_007_123_000) == true
      assert ProcessStatus.processed?("posts", 1_422_057_007_123_000) == true
    end
  end
end
