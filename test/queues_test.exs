defmodule QueuesTest do
  use ExUnit.Case, async: false

  alias EventStreamex.Operators.Queue
  alias EventStreamex.Operators.Queue.MemAdapter

  setup do
    Queue.reset_queue()

    on_exit(fn -> Queue.reset_queue() end)

    {:ok, %{}}
  end

  describe "Queue" do
    test "is empty" do
      assert match?(nil, Queue.get_task())
      assert match?({:ok, []}, MemAdapter.load_queue())
    end

    test "has 1 item" do
      Queue.enqueue("ModuleName", %WalEx.Event{type: :insert})

      assert match?([{_, {"ModuleName", %WalEx.Event{type: :insert}}}], Queue.get_queue())
      assert match?({"ModuleName", %WalEx.Event{type: :insert}}, Queue.get_task())

      assert match?(
               {:ok, [{_, {"ModuleName", %WalEx.Event{type: :insert}}}]},
               MemAdapter.load_queue()
             )
    end

    test "has 3 items" do
      Queue.enqueue("ModuleName1", %WalEx.Event{type: :insert})
      Queue.enqueue("ModuleName2", %WalEx.Event{type: :update})
      Queue.enqueue("ModuleName3", %WalEx.Event{type: :delete})

      assert match?(
               [
                 {_, {"ModuleName1", %WalEx.Event{type: :insert}}},
                 {_, {"ModuleName2", %WalEx.Event{type: :update}}},
                 {_, {"ModuleName3", %WalEx.Event{type: :delete}}}
               ],
               Queue.get_queue()
             )

      assert match?({"ModuleName1", %WalEx.Event{type: :insert}}, Queue.get_task())

      assert match?(
               {:ok,
                [
                  {_, {"ModuleName1", %WalEx.Event{type: :insert}}},
                  {_, {"ModuleName2", %WalEx.Event{type: :update}}},
                  {_, {"ModuleName3", %WalEx.Event{type: :delete}}}
                ]},
               MemAdapter.load_queue()
             )
    end

    test "task completed" do
      Queue.enqueue("ModuleName1", %WalEx.Event{type: :insert})
      Queue.enqueue("ModuleName2", %WalEx.Event{type: :update})
      Queue.enqueue("ModuleName3", %WalEx.Event{type: :delete})

      Queue.task_finished()

      assert match?(
               [
                 {_, {"ModuleName2", %WalEx.Event{type: :update}}},
                 {_, {"ModuleName3", %WalEx.Event{type: :delete}}}
               ],
               Queue.get_queue()
             )

      assert match?({"ModuleName2", %WalEx.Event{type: :update}}, Queue.get_task())

      assert match?(
               {:ok,
                [
                  {_, {"ModuleName2", %WalEx.Event{type: :update}}},
                  {_, {"ModuleName3", %WalEx.Event{type: :delete}}}
                ]},
               MemAdapter.load_queue()
             )
    end
  end
end
