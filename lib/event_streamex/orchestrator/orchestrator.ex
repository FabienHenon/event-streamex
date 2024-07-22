defmodule EventStreamex.Orchestrator do
  use GenServer
  import Bitwise

  @bits_offset 4

  defmodule NodeState do
    defstruct is_master: false, unique_identifier: nil
  end

  defstruct nodes: [], master: nil, supervisor: nil, unique_identifier: nil, nodes_state: []

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def supervisor_alive?() do
    GenServer.call(__MODULE__, :alive?)
  end

  defp broadcast(nodes, message) do
    nodes
    |> Enum.each(&send_message(&1, message))
  end

  defp send_message(node, message) do
    try do
      GenServer.cast({__MODULE__, node}, message)
    rescue
      _ ->
        :ok
    end
  end

  defp start_supervisor() do
    {:ok, pid} = EventStreamex.start_link([])
    pid
  end

  defp stop_supervisor(pid) do
    Supervisor.stop(pid, :normal)
  end

  defp whois_master?(nodes_state) do
    nodes_state
    |> Enum.reduce([], fn
      {_node, nil}, master_node ->
        master_node

      {node, %NodeState{is_master: true} = node_state}, master_node ->
        [{node, node_state} | master_node]

      {_node, %NodeState{is_master: false}}, master_node ->
        master_node
    end)
    |> Enum.uniq_by(&elem(&1, 0))
  end

  defp all_nodes_states_known?(nodes_state) do
    nodes_state
    |> Enum.all?(&(not is_nil(elem(&1, 1))))
  end

  defp elect_master(time \\ 0), do: Process.send_after(self(), :elect_master, time)

  defp broadcast_state(
         %__MODULE__{
           nodes: nodes,
           master: master,
           unique_identifier: unique_identifier
         } = state
       ) do
    IO.inspect({:set_state, master == node(), unique_identifier}, label: "BROADCASTING STATE")

    broadcast(
      [node() | nodes],
      {:set_state, node(), build_node_state(state)}
    )
  end

  defp build_node_state(%__MODULE__{master: master, unique_identifier: unique_identifier}) do
    %NodeState{is_master: master == node(), unique_identifier: unique_identifier}
  end

  defp select_best_master_node(nodes_state) do
    nodes_state
    |> IO.inspect(label: "UNIQUE IDENTIFIERS")
    |> Enum.reduce(nil, fn
      {node, %NodeState{unique_identifier: unique_identifier}}, nil ->
        {node, unique_identifier}

      {node, %NodeState{unique_identifier: unique_identifier}},
      {_curr_selected_node, selected_unique_identifier}
      when unique_identifier < selected_unique_identifier ->
        {node, unique_identifier}

      {_node, _unique_identifier}, {curr_selected_node, selected_unique_identifier} ->
        {curr_selected_node, selected_unique_identifier}
    end)
    |> elem(0)
  end

  defp exec(%__MODULE__{master: node}, :is_master?) do
    node == node()
  end

  defp exec(%__MODULE__{unique_identifier: unique_identifier}, :unique_identifier) do
    unique_identifier
  end

  defp update_node_state({node, _state}, node, node_state) do
    {node, node_state}
  end

  defp update_node_state({node1, state}, _node2, _node_state) do
    {node1, state}
  end

  # Callbacks

  @impl true
  def init(_opts) do
    :ok = :net_kernel.monitor_nodes(true)

    unique_identifier =
      IO.inspect(System.os_time(:nanosecond) <<< @bits_offset, label: "TIME") +
        IO.inspect(:rand.uniform(1 <<< @bits_offset) - 1, label: "RAND")

    IO.inspect({node(), unique_identifier, Node.list()}, label: "Nodes: ")

    nodes = Node.list()

    state = %__MODULE__{
      nodes: nodes,
      master: nil,
      supervisor: nil,
      unique_identifier: unique_identifier,
      nodes_state: nodes |> Enum.map(fn node -> {node, nil} end)
    }

    new_state = %__MODULE__{
      state
      | nodes_state: [
          {node(), build_node_state(state)} | state.nodes_state
        ]
    }

    broadcast_state(new_state)

    elect_master()

    {:ok, new_state}
  end

  @impl true
  def handle_info({:nodeup, node}, state) do
    IO.inspect({node, {node(), Node.list()}}, label: "New node connected")

    # Sending our node state to the newly connected node
    send_message(node, {:set_state, node(), build_node_state(state)})

    elect_master()

    {:noreply,
     %__MODULE__{state | nodes: Node.list(), nodes_state: [{node, nil} | state.nodes_state]}}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    IO.inspect({node, {node(), Node.list()}}, label: "Node disconnected")

    elect_master()

    IO.inspect("ELECTION DONE")

    {:noreply,
     %__MODULE__{
       state
       | nodes: Node.list(),
         nodes_state: state.nodes_state |> Enum.reject(fn {n, _state} -> n == node end)
     }}
  end

  @impl true
  def handle_info(:elect_master, %__MODULE__{nodes: nodes, nodes_state: nodes_state} = state) do
    total_nodes = [node() | nodes]
    IO.inspect(total_nodes, label: "STARTING ELECTION BETWEEN NODES")

    if all_nodes_states_known?(nodes_state) do
      case whois_master?(nodes_state) |> IO.inspect(label: "WHOIS MASTER") do
        [] ->
          # There is no master, we select the best one
          master_node =
            nodes_state
            |> select_best_master_node()
            |> IO.inspect(label: "BEST MASTER SELECTED (no previous master)")

          broadcast(total_nodes, {:set_master, master_node})

        [{master_node, _node_state} | []] ->
          broadcast(total_nodes, {:set_master, master_node})

        conflicting_master_nodes ->
          master_node =
            conflicting_master_nodes
            |> select_best_master_node()
            |> IO.inspect(label: "BEST MASTER SELECTED")

          broadcast(total_nodes, {:set_master, master_node})
      end

      IO.inspect("ELECTION DONE")

      {:noreply, state}
    else
      IO.inspect(nodes_state, label: "ALL NODES ARE NOT READY, WAITING...")
      elect_master(100)
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def handle_call(:alive?, _from, %__MODULE__{supervisor: nil} = state) do
    {:reply, false, state}
  end

  @impl true
  def handle_call(:alive?, _from, %__MODULE__{supervisor: pid} = state) do
    {:reply, Process.alive?(pid), state}
  end

  @impl true
  def handle_call(message, _from, state) do
    {:reply, exec(state, message), state}
  end

  @impl true
  def handle_cast(
        {:set_state, node, %NodeState{} = node_state},
        %__MODULE__{nodes_state: nodes_state} = state
      ) do
    IO.inspect({node, node_state}, label: "RECEIVED NODE STATE")

    {:noreply,
     %__MODULE__{
       state
       | nodes_state: nodes_state |> Enum.map(&update_node_state(&1, node, node_state))
     }
     |> IO.inspect(label: "NEW NODES_STATE")}
  end

  @impl true
  def handle_cast(
        {:set_master, new_master_node},
        %__MODULE__{master: prev_master_node, supervisor: supervisor} = state
      ) do
    IO.inspect(new_master_node, label: "NEW MASTER NODE ELECTED")

    new_state =
      cond do
        new_master_node == prev_master_node ->
          # Nothing to change
          state

        new_master_node == node() ->
          # We become the new master node, we have to start the supervisor
          %__MODULE__{
            state
            | master: new_master_node,
              supervisor: start_supervisor() |> IO.inspect(label: "SUPERVISOR STARTED")
          }

        prev_master_node == node() ->
          # We are not the master node anymore, we stop the supervisor
          stop_supervisor(supervisor) |> IO.inspect(label: "SUPERVISOR STOPPED")
          %__MODULE__{state | master: new_master_node, supervisor: nil}

        true ->
          %__MODULE__{state | master: new_master_node}
      end

    broadcast_state(new_state)

    {:noreply, new_state}
  end
end
