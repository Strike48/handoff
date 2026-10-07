defmodule Handoff.ResultStore do
  @moduledoc """
  Provides storage and retrieval of function execution results and cached arguments.

  Maintains an ETS table for fast access to results and cached arguments by ID.
  The store can also fetch data from remote nodes when needed using the DataLocationRegistry.
  """

  use GenServer

  require Logger

  @table :handoff_results

  # Client API

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Stores the result of a function execution or caches an argument for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the function result or argument
  - value: The value to store
  """
  def store(dag_id, id, value) do
    GenServer.call(__MODULE__, {:store, dag_id, id, value})
  end

  @doc """
  Stores a result without ever crashing the caller if the store is unresponsive.

  `store/3` is a blocking `GenServer.call`; a timeout *exits* the calling
  process with `{:timeout, {GenServer, :call, [__MODULE__, _msg, _t]}}`. On the
  DAG execution path that exit kills the whole execution, surfaced to callers
  as `{:error, {:execution_crashed, reason}}` (Strike48/matrix#3475).

  This variant makes ONE call with the whole time budget and returns an error
  tuple instead of exiting when the store stays unresponsive, so a slow store
  degrades throughput instead of crashing executions. It does not retry: a
  timed-out call leaves its message in the store's mailbox, so a retry would
  queue a second full copy of the value behind the first.

  Returns:
  - `:ok` if the value was stored
  - `{:error, :store_timeout}` if the store did not answer within the budget
  - `{:error, :store_unavailable}` if the store process is not running or
    exited during the call

  The budget is `:result_store_timeout` (default 5000ms) times
  `:result_store_attempts` (default 3), both overridable via the application
  environment, which tests use to fail fast.
  """
  def store_safe(dag_id, id, value) do
    GenServer.call(__MODULE__, {:store, dag_id, id, value}, store_attempts() * store_timeout())
  catch
    :exit, {:timeout, {GenServer, :call, _}} ->
      Logger.error(
        "Handoff.ResultStore unresponsive; " <>
          "result for dag_id=#{inspect(dag_id)} id=#{inspect(id)} was NOT stored"
      )

      {:error, :store_timeout}

    # A dead name exits with {:noproc, {GenServer, :call, _}}; a store that is
    # restarted mid-call exits with {:shutdown | :killed | reason, ...}.
    :exit, reason ->
      Logger.error(
        "Handoff.ResultStore unavailable (#{inspect(reason)}); " <>
          "result for dag_id=#{inspect(dag_id)} id=#{inspect(id)} was NOT stored"
      )

      {:error, :store_unavailable}
  end

  @doc """
  Retrieves a value by its ID from the local store for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the value to retrieve

  ## Returns
  - `{:ok, value}` if the value is found
  - `{:error, :not_found}` if no value exists for the ID in the given DAG
  """
  # Read the ETS table directly instead of queueing a GenServer.call behind
  # every other reader and writer. The store is a single serialized process;
  # routing O(reads) through its mailbox is what let a slow handler back up
  # the queue until callers timed out (Strike48/matrix#3475). Writes still go
  # through the GenServer, so the table stays `:protected` (owner writes,
  # everyone reads). Falls back to the GenServer if the table is not
  # available (store process restarting).
  def get(dag_id, id) do
    case :ets.lookup(@table, {dag_id, id}) do
      [{{^dag_id, ^id}, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
  rescue
    ArgumentError -> GenServer.call(__MODULE__, {:get, dag_id, id})
  end

  @doc """
  Retrieves a value, fetching it from a remote node if necessary for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the value to retrieve
  - from_node: Optional node to fetch from directly

  ## Returns
  - `{:ok, value}` if the value is found or successfully fetched
  - `{:error, :not_found}` if the value couldn't be found
  - `{:error, reason}` for other errors
  """
  def get_with_fetch(dag_id, id, from_node \\ nil) do
    # First check locally
    case get(dag_id, id) do
      {:ok, value} ->
        {:ok, value}

      {:error, :not_found} ->
        fetch_remote(dag_id, id, from_node)
    end
  end

  @doc """
  Fetches a value from a remote node and stores it locally for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID of the value to fetch
  - from_node: Specific node to fetch from, or nil to look up in registry

  ## Returns
  - `{:ok, value}` if successfully fetched
  - `{:error, reason}` if fetch failed
  """
  def fetch_remote(dag_id, id, from_node \\ nil) do
    # If not given a specific node, look up in registry
    # DataLocationRegistry lookup needs to be updated for dag_id as well
    source_node =
      if from_node do
        from_node
      else
        case Handoff.DataLocationRegistry.lookup(dag_id, id) do
          {:ok, node_id} -> node_id
          {:error, :not_found} -> nil
        end
      end

    if source_node && source_node != Node.self() do
      # Try to fetch from the source node
      # The remote :get call must also pass dag_id
      case :rpc.call(source_node, __MODULE__, :get, [dag_id, id]) do
        {:ok, value} ->
          # Cache the fetched value locally (best effort: the fetched value is
          # returned to the caller regardless of whether the cache write
          # lands, and store_safe keeps a slow store from exiting this
          # process — Strike48/matrix#3475).
          store_safe(dag_id, id, value)
          {:ok, value}

        {:error, reason} ->
          {:error, reason}

        {:badrpc, reason} ->
          Logger.error(
            "Failed to fetch value #{inspect(id)} for DAG #{inspect(dag_id)} from node #{inspect(source_node)}: #{inspect(reason)}"
          )

          {:error, reason}
      end
    else
      {:error, :not_found}
    end
  end

  @doc """
  Checks if a value exists for the given ID in a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG
  - id: The ID to check

  ## Returns
  - true if a value exists
  - false otherwise
  """
  def has_value?(dag_id, id) do
    :ets.member(@table, {dag_id, id})
  rescue
    ArgumentError -> GenServer.call(__MODULE__, {:has_value, dag_id, id})
  end

  @doc """
  Clears all stored values for a specific DAG.

  ## Parameters
  - dag_id: The ID of the DAG whose results to clear
  """
  def clear(dag_id) do
    GenServer.call(__MODULE__, {:clear, dag_id})
  end

  @doc """
  Like `clear/1`, but never exits the caller. One call bounded by
  `:result_store_timeout` (default 5000ms); returns `{:error, :store_timeout}`
  or `{:error, :store_unavailable}` instead of exiting. A timed-out clear is
  still in the store's mailbox and runs once the store catches up, so there
  is nothing to retry.
  """
  def clear_safe(dag_id) do
    GenServer.call(__MODULE__, {:clear, dag_id}, store_timeout())
  catch
    :exit, {:timeout, {GenServer, :call, _}} -> {:error, :store_timeout}
    :exit, _reason -> {:error, :store_unavailable}
  end

  # Server callbacks

  @impl true
  def init(_) do
    Logger.info("ResultStore init: #{inspect({self(), Node.self()})}")
    # :protected — the owning store process writes, any process may read (see
    # the direct-read fast path in get/2 and has_value?/2).
    table = :ets.new(@table, [:set, :protected, :named_table, read_concurrency: true])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:store, dag_id, id, value}, _from, state) do
    :ets.insert(state.table, {{dag_id, id}, value})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:get, dag_id, id}, _from, state) do
    # Never dump the whole table here: `:ets.tab2list/1` copies every stored
    # result (including large task outputs) and `inspect/1` stringifies it,
    # all inside this single serialized GenServer. Under concurrent load that
    # made every :get O(table size) and backed up the mailbox until :store
    # callers timed out at 5s (Strike48/matrix#3475). Debug logging is
    # opt-in via `config :handoff, debug_result_store: true` and logs only the
    # requested key.
    if Application.get_env(:handoff, :debug_result_store, false) do
      Logger.debug("ResultStore get: dag_id=#{inspect(dag_id)} id=#{inspect(id)}")
    end

    result =
      case :ets.lookup(state.table, {dag_id, id}) do
        [{{^dag_id, ^id}, value}] -> {:ok, value}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:has_value, dag_id, id}, _from, state) do
    result = :ets.member(state.table, {dag_id, id})
    {:reply, result, state}
  end

  @impl true
  def handle_call({:clear, dag_id}, _from, state) do
    match_spec = [{{{dag_id, :_}, :_}, [], [true]}]
    :ets.select_delete(state.table, match_spec)
    {:reply, :ok, state}
  end

  # Private helpers

  defp store_attempts, do: Application.get_env(:handoff, :result_store_attempts, 3)
  defp store_timeout, do: Application.get_env(:handoff, :result_store_timeout, 5_000)
end
