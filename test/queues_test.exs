defmodule QueuesTest do
  use ExUnit.Case, async: false

  @moduletag :queue

  alias EventStreamex.Operators.Queue
  alias EventStreamex.Operators.Queue.NoAdapter

  setup do
    Queue.reset_queue()

    on_exit(fn -> Queue.reset_queue() end)

    {:ok, %{}}
  end

  describe "Queue" do
    test "is empty" do
      assert match?(nil, Queue.get_task())
      assert match?({:ok, []}, NoAdapter.load_queue())
    end

    test "has 1 item" do
      Queue.enqueue(["ModuleName"], %WalEx.Event{type: :insert})

      assert match?(
               [{_, {%{"ModuleName" => false}, %WalEx.Event{type: :insert}}}],
               Queue.get_queue()
             )

      assert match?({%{"ModuleName" => false}, %WalEx.Event{type: :insert}}, Queue.get_task())
    end

    test "has 3 items" do
      Queue.enqueue(["ModuleName1"], %WalEx.Event{type: :insert})
      Queue.enqueue(["ModuleName2"], %WalEx.Event{type: :update})
      Queue.enqueue(["ModuleName3"], %WalEx.Event{type: :delete})

      assert match?(
               [
                 {_, {%{"ModuleName1" => false}, %WalEx.Event{type: :insert}}},
                 {_, {%{"ModuleName2" => false}, %WalEx.Event{type: :update}}},
                 {_, {%{"ModuleName3" => false}, %WalEx.Event{type: :delete}}}
               ],
               Queue.get_queue()
             )

      assert match?({%{"ModuleName1" => false}, %WalEx.Event{type: :insert}}, Queue.get_task())
    end

    test "has several processors" do
      Queue.enqueue(["ModuleName1", "ModuleName2"], %WalEx.Event{type: :insert})
      Queue.enqueue(["ModuleName2", "ModuleName2_1"], %WalEx.Event{type: :update})
      Queue.enqueue(["ModuleName3"], %WalEx.Event{type: :delete})

      assert match?(
               [
                 {_,
                  {%{"ModuleName1" => false, "ModuleName2" => false}, %WalEx.Event{type: :insert}}},
                 {_,
                  {%{"ModuleName2" => false, "ModuleName2_1" => false},
                   %WalEx.Event{type: :update}}},
                 {_, {%{"ModuleName3" => false}, %WalEx.Event{type: :delete}}}
               ],
               Queue.get_queue()
             )

      assert match?(
               {%{"ModuleName1" => false, "ModuleName2" => false}, %WalEx.Event{type: :insert}},
               Queue.get_task()
             )
    end

    test "task completed" do
      Queue.enqueue(["ModuleName1"], %WalEx.Event{type: :insert})
      Queue.enqueue(["ModuleName2"], %WalEx.Event{type: :update})
      Queue.enqueue(["ModuleName3"], %WalEx.Event{type: :delete})

      Queue.task_finished("ModuleName1")

      assert match?(
               [
                 {_, {%{"ModuleName2" => false}, %WalEx.Event{type: :update}}},
                 {_, {%{"ModuleName3" => false}, %WalEx.Event{type: :delete}}}
               ],
               Queue.get_queue()
             )

      assert match?({%{"ModuleName2" => false}, %WalEx.Event{type: :update}}, Queue.get_task())
    end
  end

  test "processor completed in task" do
    Queue.enqueue(["ModuleName1", "ModuleName2", "ModuleName3"], %WalEx.Event{type: :insert})
    Queue.enqueue(["ModuleName2"], %WalEx.Event{type: :update})
    Queue.enqueue(["ModuleName3"], %WalEx.Event{type: :delete})

    Queue.task_finished("ModuleName1")

    assert match?(
             [
               {_,
                {%{"ModuleName1" => true, "ModuleName2" => false, "ModuleName3" => false},
                 %WalEx.Event{type: :insert}}},
               {_, {%{"ModuleName2" => false}, %WalEx.Event{type: :update}}},
               {_, {%{"ModuleName3" => false}, %WalEx.Event{type: :delete}}}
             ],
             Queue.get_queue()
           )

    assert match?(
             {%{"ModuleName1" => true, "ModuleName2" => false, "ModuleName3" => false},
              %WalEx.Event{type: :insert}},
             Queue.get_task()
           )

    Queue.task_finished("ModuleName3")

    assert match?(
             [
               {_,
                {%{"ModuleName1" => true, "ModuleName2" => false, "ModuleName3" => true},
                 %WalEx.Event{type: :insert}}},
               {_, {%{"ModuleName2" => false}, %WalEx.Event{type: :update}}},
               {_, {%{"ModuleName3" => false}, %WalEx.Event{type: :delete}}}
             ],
             Queue.get_queue()
           )

    assert match?(
             {%{"ModuleName1" => true, "ModuleName2" => false, "ModuleName3" => true},
              %WalEx.Event{type: :insert}},
             Queue.get_task()
           )

    Queue.task_finished("ModuleName3")

    assert match?(
             [
               {_,
                {%{"ModuleName1" => true, "ModuleName2" => false, "ModuleName3" => true},
                 %WalEx.Event{type: :insert}}},
               {_, {%{"ModuleName2" => false}, %WalEx.Event{type: :update}}},
               {_, {%{"ModuleName3" => false}, %WalEx.Event{type: :delete}}}
             ],
             Queue.get_queue()
           )

    assert match?(
             {%{"ModuleName1" => true, "ModuleName2" => false, "ModuleName3" => true},
              %WalEx.Event{type: :insert}},
             Queue.get_task()
           )

    Queue.task_finished("ModuleName2")

    assert match?(
             [
               {_, {%{"ModuleName2" => false}, %WalEx.Event{type: :update}}},
               {_, {%{"ModuleName3" => false}, %WalEx.Event{type: :delete}}}
             ],
             Queue.get_queue()
           )

    assert match?(
             {%{"ModuleName2" => false}, %WalEx.Event{type: :update}},
             Queue.get_task()
           )
  end
end
