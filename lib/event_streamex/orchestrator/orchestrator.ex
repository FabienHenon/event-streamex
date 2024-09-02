defmodule EventStreamex.Orchestrator do
  @moduledoc """
  Module responsible for launching the event streaming system.

  The event streaming system cannot run at the same time in different nodes.
  Otherwise it would crash because of a duplicate replica slot.
  And if we changed the name of the replica slot for each node it would, then,
  duplicate all WAL events received by PostgreSQL.

  This is something we don't want, thus, we have this orchestrator that will
  ensure only one instance of the system is running at the same time.

  It will also ensure that if the master node (the one running the system) crashes or terminates,
  the system will be started on another node to continue processing new WAL events.

  Every node runs the orchestrator.
  When a node starts it generates a unique identifier based on the current time
  and communicates that id to the other nodes.

  When a node terminates a new election is run based on the lowest node identifier.
  Thus, all nodes will agree on the new master node.
  """

  @moduledoc since: "1.0.0"

  use GenServer
  import Bitwise
  require Logger

  @bits_offset 4

  defmodule NodeState do
    @moduledoc false
    defstruct is_master: false, unique_identifier: nil
  end

  defstruct nodes: [], master: nil, supervisor: nil, unique_identifier: nil, nodes_state: []

  @doc false
  @spec start_link(any()) :: :ignore | {:error, any()} | {:ok, pid()}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Checks if the supervisor is alive.

  The supervisor is the event streaming system.
  It should be alive if the current node is the master node.
  """
  @doc since: "1.0.0"
  @spec supervisor_alive?() :: boolean()
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
           master: _master,
           unique_identifier: _unique_identifier
         } = state
       ) do
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

  @doc false
  @impl true
  def init(_opts) do
    :ok = :net_kernel.monitor_nodes(true)

    unique_identifier =
      (System.os_time(:nanosecond) <<< @bits_offset) +
        (:rand.uniform(1 <<< @bits_offset) - 1)

    Logger.debug("Orchestrator initializing: #{inspect(node())}, #{unique_identifier}")

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

  @doc false
  @impl true
  def handle_info({:nodeup, node}, state) do
    Logger.debug("Orchestrator - New node connected: #{inspect({node, {node(), Node.list()}})}")

    # Sending our node state to the newly connected node
    send_message(node, {:set_state, node(), build_node_state(state)})

    elect_master()

    {:noreply,
     %__MODULE__{state | nodes: Node.list(), nodes_state: [{node, nil} | state.nodes_state]}}
  end

  @doc false
  @impl true
  def handle_info({:nodedown, node}, state) do
    Logger.debug("Orchestrator - Node disconnected: #{inspect({node, {node(), Node.list()}})}")

    elect_master()

    {:noreply,
     %__MODULE__{
       state
       | nodes: Node.list(),
         nodes_state: state.nodes_state |> Enum.reject(fn {n, _state} -> n == node end)
     }}
  end

  @doc false
  @impl true
  def handle_info(:elect_master, %__MODULE__{nodes: nodes, nodes_state: nodes_state} = state) do
    total_nodes = [node() | nodes]

    if all_nodes_states_known?(nodes_state) do
      case whois_master?(nodes_state) do
        [] ->
          # There is no master, we select the best one
          master_node =
            nodes_state
            |> select_best_master_node()

          Logger.info(
            "Orchestrator - New master elected: #{inspect(master_node)} (no previous master)"
          )

          broadcast(total_nodes, {:set_master, master_node})

        [{master_node, _node_state} | []] ->
          Logger.info("Orchestrator - New master elected: #{inspect(master_node)} (only choice)")

          broadcast(total_nodes, {:set_master, master_node})

        conflicting_master_nodes ->
          master_node =
            conflicting_master_nodes
            |> select_best_master_node()

          Logger.info(
            "Orchestrator - New master elected: #{inspect(master_node)} (between nodes: #{inspect(conflicting_master_nodes)})"
          )

          broadcast(total_nodes, {:set_master, master_node})
      end

      {:noreply, state}
    else
      Logger.debug("Orchestrator - Not ready for master election yet")
      elect_master(100)
      {:noreply, state}
    end
  end

  @doc false
  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @doc false
  @impl true
  def handle_call(:alive?, _from, %__MODULE__{supervisor: nil} = state) do
    {:reply, false, state}
  end

  @doc false
  @impl true
  def handle_call(:alive?, _from, %__MODULE__{supervisor: pid} = state) do
    {:reply, Process.alive?(pid), state}
  end

  @doc false
  @impl true
  def handle_call(message, _from, state) do
    {:reply, exec(state, message), state}
  end

  @doc false
  @impl true
  def handle_cast(
        {:set_state, node, %NodeState{} = node_state},
        %__MODULE__{nodes_state: nodes_state} = state
      ) do
    new_nodes_state = nodes_state |> Enum.map(&update_node_state(&1, node, node_state))

    Logger.debug("Orchestrator - Updated nodes state: #{inspect(new_nodes_state)}")

    {:noreply,
     %__MODULE__{
       state
       | nodes_state: new_nodes_state
     }}
  end

  @doc false
  @impl true
  def handle_cast(
        {:set_master, new_master_node},
        %__MODULE__{master: prev_master_node, supervisor: supervisor} = state
      ) do
    new_state =
      cond do
        new_master_node == prev_master_node ->
          # Nothing to change
          state

        new_master_node == node() ->
          Logger.info("Orchestrator - Starting supervisor (node becoming new master)")
          # We become the new master node, we have to start the supervisor
          %__MODULE__{
            state
            | master: new_master_node,
              supervisor: start_supervisor()
          }

        prev_master_node == node() ->
          Logger.info("Orchestrator - Stopping supervisor (node not master anymore)")
          # We are not the master node anymore, we stop the supervisor
          stop_supervisor(supervisor)

          %__MODULE__{state | master: new_master_node, supervisor: nil}

        true ->
          %__MODULE__{state | master: new_master_node}
      end

    broadcast_state(new_state)

    {:noreply, new_state}
  end
end
