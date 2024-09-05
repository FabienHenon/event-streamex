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
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123"},
                   subscribed?: %{:direct => false, :unscoped => false, "post_id:posts" => true}
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test)

      assert_receive :test, 1000
    end

    test "terminate/2", %{socket: socket} do
      {:ok, new_socket} = ScopeLiveView.mount(%{}, %{}, socket)
      {:noreply, new_socket} = ScopeLiveView.handle_params(%{"post_id" => "123"}, %{}, new_socket)
      new_socket = ScopeLiveView.terminate(:normal, new_socket)

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123"},
                   subscribed?: %{:direct => false, :unscoped => false, "post_id:posts" => false}
                 }
               }
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
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456"},
                   subscribed?: %{
                     :direct => false,
                     :unscoped => false,
                     "post_id:posts/user_id:authors" => true
                   }
                 }
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
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456"},
                   subscribed?: %{
                     :direct => false,
                     :unscoped => false,
                     "post_id:posts/user_id:authors" => false
                   }
                 }
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

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{subscribed?: %{direct: false, unscoped: true}, event_params: %{}}
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "comments", :test)

      assert_receive :test, 1000
    end

    test "terminate/2", %{socket: socket} do
      {:ok, new_socket} = UnscopeLiveView.mount(%{}, %{}, socket)

      new_socket = UnscopeLiveView.terminate(:normal, new_socket)

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   subscribed?: %{direct: false, unscoped: false},
                   event_params: %{}
                 }
               }
             }

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
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"id" => "89"},
                   subscribed?: %{:direct => true, :unscoped => false}
                 }
               }
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
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"id" => "89"},
                   subscribed?: %{:direct => false, :unscoped => false}
                 }
               }
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

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{subscribed?: %{direct: false, unscoped: true}, event_params: %{}}
               }
             }

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

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{subscribed?: %{direct: false, unscoped: true}, event_params: %{}}
               }
             }

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

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{subscribed?: %{direct: false, unscoped: true}, event_params: %{}}
               }
             }

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
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"id" => "89"},
                   subscribed?: %{direct: true, unscoped: true}
                 }
               }
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
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 }
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
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456"},
                   subscribed?: %{
                     :direct => false,
                     :unscoped => false,
                     "post_id:posts/user_id:authors" => false
                   }
                 }
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

  defmodule AllScopesAndExternalSubscriptionLiveView do
    use Phoenix.LiveView

    use EventStreamex.EventListener,
      schema: "comments",
      subscriptions: [:direct, :unscoped, %{scopes: [post_id: "posts", user_id: "authors"]}]
  end

  describe "AllScopesAndExternalSubscriptionLiveView" do
    setup %{socket: socket} do
      {:ok, new_socket} = AllScopesAndExternalSubscriptionLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        AllScopesAndExternalSubscriptionLiveView.handle_params(
          %{"post_id" => "123", "user_id" => "456", "id" => "89"},
          %{},
          new_socket
        )

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/authors/456/comments", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "comments", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "comments/89", :test_direct)

      assert_receive :test_scopes, 1000
      assert_receive :test_unscope, 1000
      assert_receive :test_direct, 1000

      %{socket: new_socket}
    end

    test "subscribe_entity/4 for unscoped users", %{socket: socket} do
      new_socket =
        AllScopesAndExternalSubscriptionLiveView.subscribe_entity(socket, "users", :unscoped)

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 },
                 "users" => %{
                   event_params: %{},
                   subscribed?: %{
                     :direct => false,
                     :unscoped => true
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      refute_receive :test_scopes, 1000
      assert_receive :test_unscope, 1000
      refute_receive :test_direct, 1000
    end

    test "subscribe_entity/4 for unknown channel", %{socket: socket} do
      new_socket =
        AllScopesAndExternalSubscriptionLiveView.subscribe_entity(socket, "users", :unknown)

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      refute_receive :test_scopes, 1000
      refute_receive :test_unscope, 1000
      refute_receive :test_direct, 1000
    end

    test "subscribe_entity/4 for direct users with missing id", %{socket: socket} do
      new_socket =
        AllScopesAndExternalSubscriptionLiveView.subscribe_entity(socket, "users", :direct)

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      refute_receive :test_scopes, 1000
      refute_receive :test_unscope, 1000
      refute_receive :test_direct, 1000
    end

    test "subscribe_entity/4 for direct users", %{socket: socket} do
      new_socket =
        AllScopesAndExternalSubscriptionLiveView.subscribe_entity(socket, "users", :direct, %{
          "id" => "91"
        })

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 },
                 "users" => %{
                   event_params: %{"id" => "91"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => false
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      refute_receive :test_scopes, 1000
      refute_receive :test_unscope, 1000
      assert_receive :test_direct, 1000
    end

    test "subscribe_entity/4 for scoped users with missing params", %{socket: socket} do
      new_socket =
        AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          socket,
          "users",
          %{scopes: [profile_id: "profiles"]},
          %{
            "id" => "91"
          }
        )

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      refute_receive :test_scopes, 1000
      refute_receive :test_unscope, 1000
      refute_receive :test_direct, 1000
    end

    test "subscribe_entity/4 for scoped users", %{socket: socket} do
      new_socket =
        AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          socket,
          "users",
          %{scopes: [profile_id: "profiles"]},
          %{
            "profile_id" => "1"
          }
        )

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 },
                 "users" => %{
                   event_params: %{"profile_id" => "1"},
                   subscribed?: %{
                     :direct => false,
                     :unscoped => false,
                     "profile_id:profiles" => true
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      assert_receive :test_scopes, 1000
      refute_receive :test_unscope, 1000
      refute_receive :test_direct, 1000
    end

    test "subscribe_entity/4 multiple scopes", %{socket: socket} do
      new_socket =
        socket
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          %{scopes: [profile_id: "profiles"]},
          %{
            "profile_id" => "1"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :direct,
          %{
            "id" => "91"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :unscoped
        )

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 },
                 "users" => %{
                   event_params: %{"id" => "91", "profile_id" => "1"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "profile_id:profiles" => true
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      assert_receive :test_scopes, 1000
      assert_receive :test_unscope, 1000
      assert_receive :test_direct, 1000
    end

    test "terminate/2", %{socket: socket} do
      new_socket =
        socket
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          %{scopes: [profile_id: "profiles"]},
          %{
            "profile_id" => "1"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :direct,
          %{
            "id" => "91"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :unscoped
        )

      new_socket = AllScopesAndExternalSubscriptionLiveView.terminate(:normal, new_socket)

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => false,
                     :unscoped => false,
                     "post_id:posts/user_id:authors" => false
                   }
                 },
                 "users" => %{
                   event_params: %{"id" => "91", "profile_id" => "1"},
                   subscribed?: %{
                     :direct => false,
                     :unscoped => false,
                     "profile_id:profiles" => false
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      refute_receive :test_scopes, 1000
      refute_receive :test_unscope, 1000
      refute_receive :test_direct, 1000
    end

    test "unsubscribe_entity/3 for unscoped", %{socket: socket} do
      new_socket =
        socket
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          %{scopes: [profile_id: "profiles"]},
          %{
            "profile_id" => "1"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :direct,
          %{
            "id" => "91"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :unscoped
        )

      new_socket =
        AllScopesAndExternalSubscriptionLiveView.unsubscribe_entity(
          new_socket,
          "users",
          :unscoped
        )

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 },
                 "users" => %{
                   event_params: %{"id" => "91", "profile_id" => "1"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => false,
                     "profile_id:profiles" => true
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      assert_receive :test_scopes, 1000
      refute_receive :test_unscope, 1000
      assert_receive :test_direct, 1000
    end

    test "unsubscribe_entity/3 for direct", %{socket: socket} do
      new_socket =
        socket
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          %{scopes: [profile_id: "profiles"]},
          %{
            "profile_id" => "1"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :direct,
          %{
            "id" => "91"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :unscoped
        )

      new_socket =
        AllScopesAndExternalSubscriptionLiveView.unsubscribe_entity(new_socket, "users", :direct)

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 },
                 "users" => %{
                   event_params: %{"id" => "91", "profile_id" => "1"},
                   subscribed?: %{
                     :direct => false,
                     :unscoped => true,
                     "profile_id:profiles" => true
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      assert_receive :test_scopes, 1000
      assert_receive :test_unscope, 1000
      refute_receive :test_direct, 1000
    end

    test "unsubscribe_entity/3 for scopes", %{socket: socket} do
      new_socket =
        socket
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          %{scopes: [profile_id: "profiles"]},
          %{
            "profile_id" => "1"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :direct,
          %{
            "id" => "91"
          }
        )
        |> AllScopesAndExternalSubscriptionLiveView.subscribe_entity(
          "users",
          :unscoped
        )

      new_socket =
        AllScopesAndExternalSubscriptionLiveView.unsubscribe_entity(new_socket, "users", %{
          scopes: [profile_id: "profiles"]
        })

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123", "user_id" => "456", "id" => "89"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "post_id:posts/user_id:authors" => true
                   }
                 },
                 "users" => %{
                   event_params: %{"id" => "91", "profile_id" => "1"},
                   subscribed?: %{
                     :direct => true,
                     :unscoped => true,
                     "profile_id:profiles" => false
                   }
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "profiles/1/users", :test_scopes)
      Utils.PubSub.broadcast(:adapter_name, "users", :test_unscope)
      Utils.PubSub.broadcast(:adapter_name, "users/91", :test_direct)

      refute_receive :test_scopes, 1000
      assert_receive :test_unscope, 1000
      assert_receive :test_direct, 1000
    end
  end
end
