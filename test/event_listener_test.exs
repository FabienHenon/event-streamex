defmodule EventListenerTest do
  use ExUnit.Case, async: false

  setup do
    %{socket: %Phoenix.LiveView.Socket{private: %{}}}
  end

  defmodule ScopeLiveView do
    use Phoenix.LiveView

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [%{scopes: [post_id: "posts"]}]
  end

  describe "ScopeLiveView" do
    test "mount/3", %{socket: socket} do
      {:ok, new_socket} = ScopeLiveView.mount(%{}, %{}, socket)

      assert new_socket.private == %{}

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test)

      refute_receive :test, 100
    end

    test "handle_params/3 with bad params", %{socket: socket} do
      {:ok, new_socket} = ScopeLiveView.mount(%{}, %{}, socket)
      {:noreply, new_socket} = ScopeLiveView.handle_params(%{"bad" => "bad"}, %{}, new_socket)

      assert new_socket.private == %{}

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test)

      refute_receive :test, 100
    end

    test "handle_params/3 with correct params", %{socket: socket} do
      {:ok, new_socket} = ScopeLiveView.mount(%{}, %{}, socket)
      {:noreply, new_socket} = ScopeLiveView.handle_params(%{"post_id" => "123"}, %{}, new_socket)

      assert new_socket.private == %{
               event_params: %{"post_id" => "123"},
               subscribed?: %{:direct => false, :unscoped => false, "post_id:posts" => true}
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test)

      assert_receive :test, 1000
    end

    test "terminate/2", %{socket: socket} do
      {:ok, new_socket} = ScopeLiveView.mount(%{}, %{}, socket)
      {:noreply, new_socket} = ScopeLiveView.handle_params(%{"post_id" => "123"}, %{}, new_socket)
      new_socket = ScopeLiveView.terminate(:normal, new_socket)

      assert new_socket.private == %{
               event_params: %{"post_id" => "123"},
               subscribed?: %{:direct => false, :unscoped => false, "post_id:posts" => false}
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test)

      refute_receive :test, 100
    end
  end

  defmodule ComplexScopeLiveView do
    use Phoenix.LiveView

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [%{scopes: [post_id: "posts", user_id: "authors"]}]
  end

  describe "ComplexScopeLiveView" do
    test "mount/3", %{socket: socket} do
      {:ok, new_socket} = ComplexScopeLiveView.mount(%{}, %{}, socket)

      assert new_socket.private == %{}

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test)

      refute_receive :test, 100
    end

    test "handle_params/3 with bad params", %{socket: socket} do
      {:ok, new_socket} = ComplexScopeLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        ComplexScopeLiveView.handle_params(%{"bad" => "bad"}, %{}, new_socket)

      assert new_socket.private == %{}

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test)

      refute_receive :test, 100
    end

    test "handle_params/3 with missing param", %{socket: socket} do
      {:ok, new_socket} = ComplexScopeLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        ComplexScopeLiveView.handle_params(%{"post_id" => "123"}, %{}, new_socket)

      assert new_socket.private == %{}

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test)

      refute_receive :test, 100
    end

    test "handle_params/3 with correct params", %{socket: socket} do
      {:ok, new_socket} = ComplexScopeLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        ComplexScopeLiveView.handle_params(
          %{"post_id" => "123", "user_id" => "456"},
          %{},
          new_socket
        )

      assert new_socket.private == %{
               event_params: %{"post_id" => "123", "user_id" => "456"},
               subscribed?: %{
                 :direct => false,
                 :unscoped => false,
                 "post_id:posts/user_id:authors" => true
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test)

      assert_receive :test, 1000
    end

    test "terminate/2", %{socket: socket} do
      {:ok, new_socket} = ComplexScopeLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        ComplexScopeLiveView.handle_params(
          %{"post_id" => "123", "user_id" => "456"},
          %{},
          new_socket
        )

      new_socket = ComplexScopeLiveView.terminate(:normal, new_socket)

      assert new_socket.private == %{
               event_params: %{"post_id" => "123", "user_id" => "456"},
               subscribed?: %{
                 :direct => false,
                 :unscoped => false,
                 "post_id:posts/user_id:authors" => false
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test)

      refute_receive :test, 100
    end
  end

  defmodule UnscopeLiveView do
    use Phoenix.LiveView

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [:unscoped]
  end

  describe "UnscopeLiveView" do
    test "mount/3", %{socket: socket} do
      {:ok, new_socket} = UnscopeLiveView.mount(%{}, %{}, socket)

      assert new_socket.private == %{subscribed?: %{direct: false, unscoped: true}}

      Utils.PubSub.broadcast(:adapter_name, "comments", :test)

      assert_receive :test, 1000
    end

    test "terminate/2", %{socket: socket} do
      {:ok, new_socket} = UnscopeLiveView.mount(%{}, %{}, socket)

      new_socket = UnscopeLiveView.terminate(:normal, new_socket)

      assert new_socket.private == %{subscribed?: %{direct: false, unscoped: false}}

      Utils.PubSub.broadcast(:adapter_name, "comments", :test)

      refute_receive :test, 100
    end
  end

  defmodule DirectScopeLiveView do
    use Phoenix.LiveView

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [:direct]
  end

  describe "DirectScopeLiveView" do
    test "mount/3", %{socket: socket} do
      {:ok, new_socket} = DirectScopeLiveView.mount(%{}, %{}, socket)

      assert new_socket.private == %{}

      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test)

      refute_receive :test, 100
    end

    test "handle_params/3 with bad params", %{socket: socket} do
      {:ok, new_socket} = DirectScopeLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        DirectScopeLiveView.handle_params(%{"bad" => "bad"}, %{}, new_socket)

      assert new_socket.private == %{}

      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test)

      refute_receive :test, 100
    end

    test "handle_params/3 with correct params", %{socket: socket} do
      {:ok, new_socket} = DirectScopeLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        DirectScopeLiveView.handle_params(%{"id" => "89"}, %{}, new_socket)

      assert new_socket.private == %{
               event_params: %{"id" => "89"},
               subscribed?: %{:direct => true, :unscoped => false}
             }

      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test)

      assert_receive :test, 1000
    end

    test "terminate/2", %{socket: socket} do
      {:ok, new_socket} = DirectScopeLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        DirectScopeLiveView.handle_params(%{"id" => "89"}, %{}, new_socket)

      new_socket = DirectScopeLiveView.terminate(:normal, new_socket)

      assert new_socket.private == %{
               event_params: %{"id" => "89"},
               subscribed?: %{:direct => false, :unscoped => false}
             }

      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test)

      refute_receive :test, 100
    end
  end

  defmodule AllScopesLiveView do
    use Phoenix.LiveView

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [:direct, :unscoped, %{scopes: [post_id: "posts", user_id: "authors"]}]
  end

  describe "AllScopesLiveView" do
    test "mount/3", %{socket: socket} do
      {:ok, new_socket} = AllScopesLiveView.mount(%{}, %{}, socket)

      assert new_socket.private == %{subscribed?: %{direct: false, unscoped: true}}

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "comments", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test_direct)

      refute_receive :test_scopes, 100
      assert_receive :test_unscope, 1000
      refute_receive :test_direct, 100
    end

    test "handle_params/3 with bad params", %{socket: socket} do
      {:ok, new_socket} = AllScopesLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        AllScopesLiveView.handle_params(%{"bad" => "bad"}, %{}, new_socket)

      assert new_socket.private == %{subscribed?: %{direct: false, unscoped: true}}

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "comments", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test_direct)

      refute_receive :test_scopes, 100
      assert_receive :test_unscope, 1000
      refute_receive :test_direct, 100
    end

    test "handle_params/3 with missing param", %{socket: socket} do
      {:ok, new_socket} = AllScopesLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        AllScopesLiveView.handle_params(%{"post_id" => "123"}, %{}, new_socket)

      assert new_socket.private == %{subscribed?: %{direct: false, unscoped: true}}

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "comments", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test_direct)

      refute_receive :test_scopes, 100
      assert_receive :test_unscope, 1000
      refute_receive :test_direct, 100
    end

    test "handle_params/3 with only direct param", %{socket: socket} do
      {:ok, new_socket} = AllScopesLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        AllScopesLiveView.handle_params(%{"post_id" => "123", "id" => "89"}, %{}, new_socket)

      assert new_socket.private == %{
               event_params: %{"id" => "89"},
               subscribed?: %{direct: true, unscoped: true}
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "comments", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test_direct)

      refute_receive :test_scopes, 100
      assert_receive :test_unscope, 1000
      assert_receive :test_direct, 1000
    end

    test "handle_params/3 with correct params", %{socket: socket} do
      {:ok, new_socket} = AllScopesLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        AllScopesLiveView.handle_params(
          %{"post_id" => "123", "user_id" => "456", "id" => "89"},
          %{},
          new_socket
        )

      assert new_socket.private == %{
               event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
               subscribed?: %{
                 :direct => true,
                 :unscoped => true,
                 "post_id:posts/user_id:authors" => true
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "comments", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test_direct)

      assert_receive :test_scopes, 1000
      assert_receive :test_unscope, 1000
      assert_receive :test_direct, 1000
    end

    test "terminate/2", %{socket: socket} do
      {:ok, new_socket} = AllScopesLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        AllScopesLiveView.handle_params(
          %{"post_id" => "123", "user_id" => "456"},
          %{},
          new_socket
        )

      new_socket = AllScopesLiveView.terminate(:normal, new_socket)

      assert new_socket.private == %{
               event_params: %{"post_id" => "123", "user_id" => "456"},
               subscribed?: %{
                 :direct => false,
                 :unscoped => false,
                 "post_id:posts/user_id:authors" => false
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "comments", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test_direct)

      refute_receive :test_scopes, 100
      refute_receive :test_unscope, 100
      refute_receive :test_direct, 100
    end
  end
end
