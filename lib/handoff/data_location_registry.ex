defmodule Handoff.DataLocationRegistry do
  @moduledoc """
  Tracks the location (node ID) of every piece of data (argument or intermediate result) for each DAG.

  This registry maintains a mapping of {dag_id, data_id} to their hosting nodes, enabling
  on-demand data fetching from the appropriate node for a specific DAG execution.

  Storage is a `:protected` ETS table: the owning process writes, any process may
  read directly. Reads used to be routed through the GenServer mailbox, and
  `clear/1` rebuilt the whole state map (O(all DAGs, tenants and concurrent
  runs)) inside that process, on every DAG start and finish — both of which
  turned a busy registry into a serialization point for the executor
  (Strike48/matrix#3475).
  """

  use GenServer

  require Logger

  @table :handoff_data_locations

  # Client API

  @doc """
  Starts the Data Location Registry GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers a data item with its hosting node for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - data_id: The ID of the data (argument or result)
  - node_id: The node where the data is stored
  """
  def register(dag_id, data_id, node_id) do
    GenServer.call(__MODULE__, {:register, dag_id, data_id, node_id})
  end

  @doc """
  Looks up where a data item is stored for a specific DAG.

  Reads the ETS table directly (lock-free) rather than queueing behind the
  registry process; falls back to the GenServer only if the table is not
  available (registry restarting).

  ## Parameters
  - dag_id: The ID of the DAG
  - data_id: The ID of the data (argument or result)

  ## Returns
  - `{:ok, node_id}` if the data location is found for the DAG
  - `{:error, :not_found}` if the data location is not registered for the DAG
  """
  def lookup(dag_id, data_id) do
    case :ets.lookup(@table, {dag_id, data_id}) do
      [{{^dag_id, ^data_id}, node_id}] -> {:ok, node_id}
      [] -> {:error, :not_found}
    end
  rescue
    ArgumentError -> GenServer.call(__MODULE__, {:lookup, dag_id, data_id})
  end

  @doc """
  Gets all registered data locations for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG

  ## Returns
  - A map of data_id => node_id for the specified DAG
  """
  def get_all(dag_id) do
    @table
    |> :ets.match_object({{dag_id, :_}, :_})
    |> Map.new(fn {{_dag_id, data_id}, node_id} -> {data_id, node_id} end)
  rescue
    ArgumentError -> GenServer.call(__MODULE__, {:get_all_for_dag, dag_id})
  end

  @doc """
  Clears all registered data locations for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  """
  def clear(dag_id) do
    GenServer.call(__MODULE__, {:clear_dag, dag_id})
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    # :protected — this process writes, any process may read (see lookup/2).
    table = :ets.new(@table, [:set, :protected, :named_table, read_concurrency: true])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:register, dag_id, data_id, node_id}, _from, %{table: table} = state) do
    Logger.debug(
      "Registering data #{inspect(data_id)} for DAG #{inspect(dag_id)} at node #{inspect(node_id)}"
    )

    :ets.insert(table, {{dag_id, data_id}, node_id})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:lookup, dag_id, data_id}, _from, %{table: table} = state) do
    result =
      case :ets.lookup(table, {dag_id, data_id}) do
        [{{^dag_id, ^data_id}, node_id}] -> {:ok, node_id}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:get_all_for_dag, dag_id}, _from, %{table: table} = state) do
    result =
      table
      |> :ets.match_object({{dag_id, :_}, :_})
      |> Map.new(fn {{_dag_id, data_id}, node_id} -> {data_id, node_id} end)

    {:reply, result, state}
  end

  @impl true
  def handle_call({:clear_dag, dag_id}, _from, %{table: table} = state) do
    # Per-DAG select_delete instead of rebuilding the whole state map: the old
    # form was O(all registered entries across every DAG) inside the process.
    :ets.select_delete(table, [{{{dag_id, :_}, :_}, [], [true]}])
    {:reply, :ok, state}
  end
end
