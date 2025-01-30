defmodule EventStreamex.Events.LiveViewMonitor do
  use GenServer

  @moduledoc """
  This GenServer will monitor every LiveView of the current node when it `use`s `EventListener`.
  A `LiveViewMonitor` is started in each of your nodes.

  When a LiveView exits, it will call `EventStreamex.EventListener.unsubscribe_all/2` which
  will unsubscribe from all channels.
  """

  @moduledoc since: "1.4.1"

  # The process is local to the node to monitor liveviews created inside the
  # node.
  # If the node is shutdown, all liveviews and the liveviewMonitor will
  # be destroyed. And that's ok
  def process_name(), do: __MODULE__

  @doc """
  Monitors a LiveView given its `pid`, its module name and its state (only the subscriptions part).

  *Only call this function when the liveview is connected to the socket*
  """
  def monitor(pid, view_module, view_state) do
    GenServer.call(process_name(), {:monitor, {pid, view_module, view_state}})
  end

  @doc """
  Updates the state of the liveview specified by `pid`.

  It's important to keep the subscriptions state up to date because it will
  be need when the process exits
  """
  def update_state(pid, view_state) do
    GenServer.call(process_name(), {:update_state, {pid, view_state}})
  end

  @doc false
  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg, name: process_name())
  end

  @doc false
  @impl true
  def init(_) do
    {:ok, %{views: %{}}}
  end

  @doc false
  @impl true
  def handle_call({:monitor, {pid, view_module, view_state}}, _, %{views: views} = state) do
    mref = Process.monitor(pid)
    {:reply, :ok, %{state | views: Map.put(views, pid, {view_module, view_state, mref})}}
  end

  @doc false
  @impl true
  def handle_call({:update_state, {pid, view_state}}, _, %{views: views} = state) do
    # If we update the state of a view not registered, we do not change the global state
    new_views =
      try do
        Map.update!(views, pid, fn {view_module, _old_state, mref} ->
          {view_module, view_state, mref}
        end)
      rescue
        _ ->
          views
      end

    {:reply, :ok,
     %{
       state
       | views: new_views
     }}
  end

  @doc false
  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    {{module, view_state, _mref}, new_views} = Map.pop(state.views, pid)

    try do
      module.unsubscribe_all(reason, view_state)
    rescue
      _ ->
        :ok
    end

    {:noreply, %{state | views: new_views}}
  end
end
