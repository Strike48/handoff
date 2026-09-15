defmodule Handoff.ResultStoreSlowStoreTest do
  @moduledoc """
  Regression tests for Strike48/matrix#3475: a slow/unresponsive
  `Handoff.ResultStore` must not crash DAG executions.

  Before the fix, the `:get` handler dumped the entire ETS table on every call
  (`:ets.tab2list` + `inspect` at INFO level), which backed up the single
  serialized store GenServer under load until fast `:store` calls timed out at
  5s. The timeout *exits* the calling process — on the DAG execution path that
  killed the execution task, surfaced to callers as
  `{:error, {:execution_crashed, {:timeout, {GenServer, :call,
  [Handoff.ResultStore, {:store, ...}, 5000]}}}}` and marked executions
  failed.

  These tests inject "slowness" by suspending the store process
  (`:sys.suspend/1`) — the exact production failure mode, where the store was
  alive but unresponsive.
  """

  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.DistributedExecutor
  alias Handoff.DistributedTestFunctions
  alias Handoff.Function
  alias Handoff.ResultStore
  alias Handoff.SimpleResourceTracker

  @dag_id "slow_store_dag"
  @large_dag_id "slow_store_large_table_dag"

  setup do
    # The executor allocates functions to nodes by capability.
    SimpleResourceTracker.register(Node.self(), %{cpu: 4, memory: 2000})

    # Fail fast against a suspended store instead of burning 5s per attempt.
    Application.put_env(:handoff, :result_store_timeout, 100)
    Application.put_env(:handoff, :result_store_attempts, 3)
    ResultStore.clear(@dag_id)
    ResultStore.clear(@large_dag_id)

    on_exit(fn ->
      Application.delete_env(:handoff, :result_store_timeout)
      Application.delete_env(:handoff, :result_store_attempts)
    end)

    :ok
  end

  defp suspend_store! do
    # :sys.suspend/1 performs the suspension as a side effect and returns
    # bare :ok — register the resume BEFORE suspending so a test abort can
    # never leak a suspended singleton into the rest of the suite.
    on_exit(&resume_store!/0)
    :ok = :sys.suspend(Handoff.ResultStore)
    :ok
  end

  defp resume_store! do
    :sys.resume(Handoff.ResultStore)
  end

  defp single_fn_dag(fn_id) do
    make_ref()
    |> DAG.new()
    |> DAG.add_function(%Function{
      id: fn_id,
      args: [],
      # DAG.add_function/2 requires a fully qualified &Module.fun/arity capture
      # (code is serialized and may execute on another node).
      code: &DistributedTestFunctions.stored_value/0,
      cost: %{cpu: 1, memory: 100}
    })
  end

  describe "store_safe/3" do
    test "stores normally when the store is healthy" do
      assert :ok = ResultStore.store_safe(@dag_id, :fn, "value")
      assert {:ok, "value"} = ResultStore.get(@dag_id, :fn)
    end

    test "degrades to an error tuple instead of exiting the caller when the store is suspended" do
      suspend_store!()

      start = System.monotonic_time(:millisecond)

      assert {:error, :store_timeout} = ResultStore.store_safe(@dag_id, :fn, "value")

      elapsed = System.monotonic_time(:millisecond) - start

      # 3 attempts x 100ms timeout + 300ms + 200ms backoff, but nowhere near
      # the old behaviour where the caller process simply died.
      assert elapsed >= 600
      assert elapsed < 3_000
      assert Process.alive?(self())
    end

    test "retries and succeeds when the store recovers mid-way" do
      suspend_store!()

      # Bring the store back during the retry window.
      Task.start(fn ->
        :timer.sleep(150)
        resume_store!()
      end)

      assert :ok = ResultStore.store_safe(@dag_id, :fn, "value")
      assert {:ok, "value"} = ResultStore.get(@dag_id, :fn)
    end
  end

  describe "DAG execution against a slow store" do
    test "a suspended store does not crash the execution with :execution_crashed" do
      suspend_store!()

      assert {:ok, %{results: results}} = DistributedExecutor.execute(single_fn_dag(:fn1))

      # The result could not be persisted, so the function is attributed with
      # a store failure — but the execution itself completes instead of
      # crashing. Pre-fix this raised/returned:
      #   {:error, {:execution_crashed,
      #            {:timeout, {GenServer, :call,
      #             [Handoff.ResultStore, {:store, _dag, :fn1, _}, 5000]}}}}
      assert {:error, {:result_store_unavailable, :store_timeout}} = results[:fn1]
    end

    test "the execution recovers when the store becomes responsive again" do
      # A fresh execution right after resuming a suspended store must be
      # fully functional again (no lost state, no crash).
      suspend_store!()
      resume_store!()

      assert {:ok, %{results: %{fn1: "stored-value"}}} =
               DistributedExecutor.execute(single_fn_dag(:fn1))
    end

    test ":get stays O(1) — no full-table scan or dump" do
      # The #3475 root cause: the :get handler copied and stringified the
      # ENTIRE table per call (:ets.tab2list + inspect). Under load every get
      # held the serialized store for O(table size), backing up :store
      # callers until they hit the 5s GenServer.call timeout.
      #
      # The test env's :warning logger level makes the pre-fix dump message
      # lazily skipped (Elixir's Logger macros skip evaluating messages above
      # the level), which would mask its cost. Run at :info — like production
      # — so the handler work actually executes.
      prev_level = Logger.level()
      :ok = Logger.configure(level: :info)
      on_exit(fn -> Logger.configure(level: prev_level) end)

      payload = String.duplicate("v", 100)

      # Other test files share this store process; don't leave 100k rows
      # behind for them.
      on_exit(fn -> ResultStore.clear(@large_dag_id) end)

      Enum.each(1..100_000, fn i ->
        ResultStore.store(@large_dag_id, i, payload)
      end)

      # Warm-up absorbs first-touch/GC noise from the inserts.
      assert {:ok, ^payload} = ResultStore.get(@large_dag_id, 1)

      start = System.monotonic_time(:millisecond)

      for _ <- 1..5 do
        {:ok, ^payload} = ResultStore.get(@large_dag_id, 50_000)
      end

      elapsed = System.monotonic_time(:millisecond) - start

      # 5 O(1) gets: well under 5ms total. Pre-fix, each get copied all 100k
      # rows out of ETS and stringified the list before replying.
      assert elapsed < 5,
             "5 :get calls on a 100k-row table took #{elapsed}ms — O(n) full-table work inside :get has returned (see Strike48/matrix#3475)"
    end
  end
end
