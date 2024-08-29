defmodule Utils do
  defmodule PubSub do
    use GenServer

    def subscribe(:adapter_name, channel) do
      GenServer.cast(__MODULE__, {:subscribe, self(), channel})
    end

    def unsubscribe(:adapter_name, channel) do
      GenServer.cast(__MODULE__, {:unsubscribe, self(), channel})
    end

    def broadcast(:adapter_name, channel, message) do
      GenServer.cast(__MODULE__, {:broadcast, channel, message})
    end

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(_opts) do
      {:ok, %{}}
    end

    @impl true
    def handle_cast({:subscribe, pid, channel}, state) do
      {:noreply, Map.put(state, channel, pid)}
    end

    @impl true
    def handle_cast({:unsubscribe, _pid, channel}, state) do
      {:noreply, Map.delete(state, channel)}
    end

    @impl true
    def handle_cast({:broadcast, channel, message}, state) do
      pid = Map.get(state, channel, nil)
      pid && Process.send(pid, message, [])
      {:noreply, state}
    end
  end

  defmodule LoggerAdapter do
    use GenServer
    @behaviour EventStreamex.Operators.Logger.ErrorLoggerAdapter

    @impl EventStreamex.Operators.Logger.ErrorLoggerAdapter
    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl EventStreamex.Operators.Logger.ErrorLoggerAdapter
    def log_retry(module, reason, retry_status) do
      GenServer.cast(__MODULE__, {:log_retry, module, reason, retry_status})
    end

    @impl EventStreamex.Operators.Logger.ErrorLoggerAdapter
    def log_failed(module, reason, retry_status) do
      GenServer.cast(__MODULE__, {:log_failed, module, reason, retry_status})
    end

    def set_parent(parent) do
      GenServer.cast(__MODULE__, {:set_parent, parent})
    end

    # Callbacks

    @impl true
    def init(parent) do
      {:ok, parent}
    end

    @impl true
    def handle_cast({:set_parent, parent}, _parent) do
      {:noreply, parent}
    end

    @impl true
    def handle_cast(_msg, nil) do
      {:noreply, nil}
    end

    @impl true
    def handle_cast({:log_retry, module, reason, retry_status}, parent) do
      Process.send(parent, {:log_retry, module, reason, retry_status}, [])

      {:noreply, parent}
    end

    @impl true
    def handle_cast({:log_failed, module, reason, retry_status}, parent) do
      Process.send(parent, {:log_failed, module, reason, retry_status}, [])

      {:noreply, parent}
    end
  end
end
