defmodule EventStreamexTest do
  use ExUnit.Case
  doctest EventStreamex

  test "greets the world" do
    assert EventStreamex.hello() == :world
  end
end
