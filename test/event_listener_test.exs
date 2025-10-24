defmodule EventListenerTest do
  use ExUnit.Case, async: false

  setup do
    %{socket: %Phoenix.LiveView.Socket{private: %{}, transport_pid: self()}}
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

    test "unsubscribe_all/2", %{socket: socket} do
      {:ok, new_socket} = ScopeLiveView.mount(%{}, %{}, socket)
      {:noreply, new_socket} = ScopeLiveView.handle_params(%{"post_id" => "123"}, %{}, new_socket)

      new_socket =
        ScopeLiveView.unsubscribe_all(:normal, Map.get(new_socket.private, :subscriptions, %{}))

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

    test "unsubscribe_all/2", %{socket: socket} do
      {:ok, new_socket} = ComplexScopeLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        ComplexScopeLiveView.handle_params(
          %{"post_id" => "123", "user_id" => "456"},
          %{},
          new_socket
        )

      new_socket =
        ComplexScopeLiveView.unsubscribe_all(
          :normal,
          Map.get(new_socket.private, :subscriptions, %{})
        )

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

    test "unsubscribe_all/2", %{socket: socket} do
      {:ok, new_socket} = UnscopeLiveView.mount(%{}, %{}, socket)

      new_socket =
        UnscopeLiveView.unsubscribe_all(:normal, Map.get(new_socket.private, :subscriptions, %{}))

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

    test "unsubscribe_all/2", %{socket: socket} do
      {:ok, new_socket} = DirectScopeLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        DirectScopeLiveView.handle_params(%{"id" => "89"}, %{}, new_socket)

      new_socket =
        DirectScopeLiveView.unsubscribe_all(
          :normal,
          Map.get(new_socket.private, :subscriptions, %{})
        )

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

    test "unsubscribe_all/2", %{socket: socket} do
      {:ok, new_socket} = AllScopesLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        AllScopesLiveView.handle_params(
          %{"post_id" => "123", "user_id" => "456"},
          %{},
          new_socket
        )

      new_socket =
        AllScopesLiveView.unsubscribe_all(
          :normal,
          Map.get(new_socket.private, :subscriptions, %{})
        )

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

      assert_receive :test_scopes, 2000
      assert_receive :test_unscope, 2000
      assert_receive :test_direct, 2000

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

    test "unsubscribe_all/2", %{socket: socket} do
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
        AllScopesAndExternalSubscriptionLiveView.unsubscribe_all(
          :normal,
          Map.get(new_socket.private, :subscriptions, %{})
        )

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

  defmodule MultiScopesLiveView do
    use Phoenix.LiveView

    use EventStreamex.EventListener,
      schemas: [
        %{schema: "comments", subscriptions: [%{scopes: [post_id: "posts"]}]},
        %{schema: "posts", subscriptions: [:unscoped]}
      ]
  end

  describe "MultiScopesLiveView" do
    test "mount/3", %{socket: socket} do
      {:ok, new_socket} = MultiScopesLiveView.mount(%{}, %{}, socket)

      assert new_socket.private == %{
               subscriptions: %{
                 "posts" => %{event_params: %{}, subscribed?: %{direct: false, unscoped: true}}
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test_comments)
      Utils.PubSub.broadcast(:adapter_name, "posts", :test_posts)

      refute_receive :test_comments, 100
      assert_receive :test_posts, 1000
    end

    test "handle_params/3 with bad params", %{socket: socket} do
      {:ok, new_socket} = MultiScopesLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        MultiScopesLiveView.handle_params(%{"bad" => "bad"}, %{}, new_socket)

      assert new_socket.private == %{
               subscriptions: %{
                 "posts" => %{event_params: %{}, subscribed?: %{direct: false, unscoped: true}}
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test_comments)
      Utils.PubSub.broadcast(:adapter_name, "posts", :test_posts)

      refute_receive :test_comments, 100
      assert_receive :test_posts, 1000
    end

    test "handle_params/3 with correct params", %{socket: socket} do
      {:ok, new_socket} = MultiScopesLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        MultiScopesLiveView.handle_params(%{"post_id" => "123"}, %{}, new_socket)

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123"},
                   subscribed?: %{:direct => false, :unscoped => false, "post_id:posts" => true}
                 },
                 "posts" => %{
                   event_params: %{},
                   subscribed?: %{:direct => false, :unscoped => true}
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test_comments)
      Utils.PubSub.broadcast(:adapter_name, "posts", :test_posts)

      assert_receive :test_comments, 1000
      assert_receive :test_posts, 1000
    end

    test "unsubscribe_all/2", %{socket: socket} do
      {:ok, new_socket} = MultiScopesLiveView.mount(%{}, %{}, socket)

      {:noreply, new_socket} =
        MultiScopesLiveView.handle_params(%{"post_id" => "123"}, %{}, new_socket)

      new_socket =
        MultiScopesLiveView.unsubscribe_all(
          :normal,
          Map.get(new_socket.private, :subscriptions, %{})
        )

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123"},
                   subscribed?: %{:direct => false, :unscoped => false, "post_id:posts" => false}
                 },
                 "posts" => %{
                   subscribed?: %{direct: false, unscoped: false},
                   event_params: %{}
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test_comments)
      Utils.PubSub.broadcast(:adapter_name, "posts", :test_posts)

      refute_receive :test_comments, 100
      refute_receive :test_posts, 100
    end
  end

  defmodule GenServerLiveView do
    use GenServer

    use EventStreamex.EventListener,
      schemas: [
        %{schema: "comments", subscriptions: [%{scopes: [post_id: "posts"]}]},
        %{schema: "posts", subscriptions: [:unscoped]}
      ]

    def start({parent_pid, socket}) do
      GenServer.start(__MODULE__, {parent_pid, socket})
    end

    def do_mount(pid, params) do
      GenServer.call(pid, {:do_mount, params})
    end

    def do_handle_params(pid, params) do
      GenServer.call(pid, {:do_handle_params, params})
    end

    @impl true
    def init({parent_pid, socket}) do
      {:ok, {parent_pid, socket}}
    end

    @impl true
    def handle_call({:do_mount, params}, _from, {parent_pid, socket}) do
      {:ok, new_socket} = mount(params, %{}, socket)

      {:reply, new_socket, {parent_pid, new_socket}}
    end

    @impl true
    def handle_call({:do_handle_params, params}, _from, {parent_pid, socket}) do
      {:noreply, new_socket} = handle_params(params, %{}, socket)

      {:reply, new_socket, {parent_pid, new_socket}}
    end

    @impl true
    def handle_info(message, {parent_pid, socket}) do
      send(parent_pid, message)
      {:noreply, {parent_pid, socket}}
    end
  end

  describe "GenServerLiveView" do
    test "Check all channels are unsubscribed when process is killed", %{socket: socket} do
      {:ok, pid} = GenServerLiveView.start({self(), socket})
      _new_socket = GenServerLiveView.do_mount(pid, %{})

      new_socket =
        GenServerLiveView.do_handle_params(pid, %{"post_id" => "123"})

      assert new_socket.private == %{
               subscriptions: %{
                 "comments" => %{
                   event_params: %{"post_id" => "123"},
                   subscribed?: %{:direct => false, :unscoped => false, "post_id:posts" => true}
                 },
                 "posts" => %{
                   event_params: %{},
                   subscribed?: %{:direct => false, :unscoped => true}
                 }
               }
             }

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test_comments)
      Utils.PubSub.broadcast(:adapter_name, "posts", :test_posts)

      assert_receive :test_comments, 1000
      assert_receive :test_posts, 1000

      Process.exit(pid, :kill)

      Utils.PubSub.broadcast(:adapter_name, "posts/123/comments", :test_comments)
      Utils.PubSub.broadcast(:adapter_name, "posts", :test_posts)

      refute_receive :test_comments, 100
      refute_receive :test_posts, 100
    end
  end

  describe "scope_params_for_schema/3" do
    defmodule TestLiveView do
      use Phoenix.LiveView

      use EventStreamex.EventListener,
        schema: "comments",
        subscriptions: [:direct]
    end

    test "scopes params under schema key with empty params map" do
      result = TestLiveView.scope_params_for_schema(%{}, "users", %{"id" => "123"})

      assert result == %{"users" => %{"id" => "123"}}
    end

    test "scopes params under schema key with existing params" do
      params = %{"other_key" => "other_value"}
      result = TestLiveView.scope_params_for_schema(params, "users", %{"id" => "123"})

      assert result == %{
               "other_key" => "other_value",
               "users" => %{"id" => "123"}
             }
    end

    test "replaces existing schema params when schema key already exists" do
      params = %{"users" => %{"id" => "456", "name" => "John"}}
      result = TestLiveView.scope_params_for_schema(params, "users", %{"id" => "123"})

      assert result == %{"users" => %{"id" => "123"}}
    end

    test "works with atom schema keys" do
      result = TestLiveView.scope_params_for_schema(%{}, :users, %{"id" => "123"})

      assert result == %{:users => %{"id" => "123"}}
    end

    test "works with string schema keys" do
      result = TestLiveView.scope_params_for_schema(%{}, "users", %{"id" => "123"})

      assert result == %{"users" => %{"id" => "123"}}
    end

    test "supports chaining multiple calls" do
      result =
        %{}
        |> TestLiveView.scope_params_for_schema("users", %{"id" => "user_123"})
        |> TestLiveView.scope_params_for_schema("posts", %{"id" => "post_456"})

      assert result == %{
               "users" => %{"id" => "user_123"},
               "posts" => %{"id" => "post_456"}
             }
    end

    test "handles complex schema params" do
      schema_params = %{
        "id" => "123",
        "name" => "John Doe",
        "settings" => %{"theme" => "dark"}
      }

      result = TestLiveView.scope_params_for_schema(%{}, "users", schema_params)

      assert result == %{"users" => schema_params}
    end

    test "handles empty schema params" do
      result = TestLiveView.scope_params_for_schema(%{"existing" => "value"}, "users", %{})

      assert result == %{
               "existing" => "value",
               "users" => %{}
             }
    end
  end

  describe "scope_params_for_schema/3 integration with LiveView functions" do
    defmodule MultiDirectLiveView do
      use Phoenix.LiveView

      use EventStreamex.EventListener,
        schemas: [
          %{schema: "users", subscriptions: [:direct]},
          %{schema: "posts", subscriptions: [:direct]}
        ]
    end

    test "mount/3 with scoped params for multiple direct subscriptions", %{socket: socket} do
      # Create scoped params for two different entities with their respective IDs
      scoped_params =
        %{}
        |> MultiDirectLiveView.scope_params_for_schema("users", %{"id" => "user_123"})
        |> MultiDirectLiveView.scope_params_for_schema("posts", %{"id" => "post_456"})

      {:ok, new_socket} = MultiDirectLiveView.mount(scoped_params, %{}, socket)

      # Both entities should be subscribed with their respective IDs
      assert new_socket.private == %{
               subscriptions: %{
                 "users" => %{
                   event_params: %{"id" => "user_123"},
                   subscribed?: %{direct: true, unscoped: false}
                 },
                 "posts" => %{
                   event_params: %{"id" => "post_456"},
                   subscribed?: %{direct: true, unscoped: false}
                 }
               }
             }

      # Test that each entity receives its own direct messages
      Utils.PubSub.broadcast(:adapter_name, "users/user_123", :test_users)
      Utils.PubSub.broadcast(:adapter_name, "posts/post_456", :test_posts)
      Utils.PubSub.broadcast(:adapter_name, "users/wrong_id", :test_wrong_users)
      Utils.PubSub.broadcast(:adapter_name, "posts/wrong_id", :test_wrong_posts)

      assert_receive :test_users, 1000
      assert_receive :test_posts, 1000
      refute_receive :test_wrong_users, 100
      refute_receive :test_wrong_posts, 100
    end

    test "handle_params/3 with scoped params for multiple direct subscriptions", %{socket: socket} do
      {:ok, new_socket} = MultiDirectLiveView.mount(%{}, %{}, socket)

      # Initially no subscriptions since no IDs were provided
      assert new_socket.private == %{}

      # Create scoped params for two different entities with their respective IDs
      scoped_params =
        %{}
        |> MultiDirectLiveView.scope_params_for_schema("users", %{"id" => "user_789"})
        |> MultiDirectLiveView.scope_params_for_schema("posts", %{"id" => "post_101"})

      {:noreply, new_socket} = MultiDirectLiveView.handle_params(scoped_params, %{}, new_socket)

      # Both entities should now be subscribed with their respective IDs
      assert new_socket.private == %{
               subscriptions: %{
                 "users" => %{
                   event_params: %{"id" => "user_789"},
                   subscribed?: %{direct: true, unscoped: false}
                 },
                 "posts" => %{
                   event_params: %{"id" => "post_101"},
                   subscribed?: %{direct: true, unscoped: false}
                 }
               }
             }

      # Test that each entity receives its own direct messages
      Utils.PubSub.broadcast(:adapter_name, "users/user_789", :test_users)
      Utils.PubSub.broadcast(:adapter_name, "posts/post_101", :test_posts)
      Utils.PubSub.broadcast(:adapter_name, "users/user_123", :test_old_users)
      Utils.PubSub.broadcast(:adapter_name, "posts/post_456", :test_old_posts)

      assert_receive :test_users, 1000
      assert_receive :test_posts, 1000
      refute_receive :test_old_users, 100
      refute_receive :test_old_posts, 100
    end

    test "mixed scoped and unscoped params", %{socket: socket} do
      # Test mixing scoped and unscoped parameters
      mixed_params =
        %{"global_setting" => "value", "other_param" => "test"}
        |> MultiDirectLiveView.scope_params_for_schema("users", %{"id" => "user_mixed"})
        |> MultiDirectLiveView.scope_params_for_schema("posts", %{"id" => "post_mixed"})

      {:ok, new_socket} = MultiDirectLiveView.mount(mixed_params, %{}, socket)

      assert new_socket.private == %{
               subscriptions: %{
                 "users" => %{
                   event_params: %{"id" => "user_mixed"},
                   subscribed?: %{direct: true, unscoped: false}
                 },
                 "posts" => %{
                   event_params: %{"id" => "post_mixed"},
                   subscribed?: %{direct: true, unscoped: false}
                 }
               }
             }

      # Test that each entity receives its own direct messages
      Utils.PubSub.broadcast(:adapter_name, "users/user_mixed", :test_users)
      Utils.PubSub.broadcast(:adapter_name, "posts/post_mixed", :test_posts)

      assert_receive :test_users, 1000
      assert_receive :test_posts, 1000
    end
  end
end
