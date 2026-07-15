defmodule Handoff.CrashIsolationTest do
  # DistributedExecutor is a singleton; these tests intentionally crash DAG
  # tasks, so keep them serialized with the other executor tests.
  use ExUnit.Case, async: false

  alias Handoff.DAG
  alias Handoff.DistributedExecutor
  alias Handoff.Function
  alias Handoff.SimpleResourceTracker

  setup do
    SimpleResourceTracker.register(Node.self(), %{cpu: 4, memory: 2000})
    :ok
  end

  defp crashing_dag do
    make_ref()
    |> DAG.new()
    |> DAG.add_function(%Function{
      id: :boom,
      args: [],
      code: &Handoff.DistributedTestFunctions.exit_abnormally/1,
      extra_args: [nil],
      cost: %{cpu: 1, memory: 100}
    })
  end

  defp slow_dag(sleep_ms) do
    make_ref()
    |> DAG.new()
    |> DAG.add_function(%Function{
      id: :slow,
      args: [],
      code: &Handoff.DistributedTestFunctions.slow_identity/2,
      extra_args: [:done, sleep_ms],
      cost: %{cpu: 1, memory: 100}
    })
  end

  test "a DAG task that exits abnormally replies an error instead of hanging the caller" do
    assert {:error, {:execution_crashed, :crash_isolation_boom}} =
             DistributedExecutor.execute(crashing_dag())
  end

  test "a crashing DAG task does not take down the executor" do
    executor_pid = Process.whereis(DistributedExecutor)
    assert is_pid(executor_pid)

    assert {:error, {:execution_crashed, _reason}} =
             DistributedExecutor.execute(crashing_dag())

    assert Process.whereis(DistributedExecutor) == executor_pid
    assert Process.alive?(executor_pid)
  end

  test "a DAG task that exits with reason :normal still replies an error (no result is a protocol violation)" do
    dag =
      make_ref()
      |> DAG.new()
      |> DAG.add_function(%Function{
        id: :normal_exit,
        args: [],
        code: &Handoff.DistributedTestFunctions.exit_normally/1,
        extra_args: [nil],
        cost: %{cpu: 1, memory: 100}
      })

    # Must reply — a caller stuck in GenServer.call/3 with :infinity would
    # otherwise hang forever.
    task = Task.async(fn -> DistributedExecutor.execute(dag) end)

    assert {:error, {:execution_crashed, :normal}} = Task.await(task, 5_000)
  end

  test "an executor restart terminates in-flight DAG tasks instead of orphaning them" do
    slow_result =
      Task.async(fn ->
        try do
          DistributedExecutor.execute(slow_dag(60_000))
        catch
          :exit, reason -> {:caller_exited, reason}
        end
      end)

    # Wait for the slow DAG task to be dispatched under the task supervisor.
    wait_until(fn ->
      Task.Supervisor.children(Handoff.DagTaskSupervisor) != []
    end)

    executor_pid = Process.whereis(DistributedExecutor)
    Process.exit(executor_pid, :kill)

    # The executor restart must take the orphaned DAG tasks down with it —
    # otherwise they keep executing side effects with nobody tracking them.
    wait_until(fn ->
      Process.whereis(DistributedExecutor) != nil and
        Process.whereis(DistributedExecutor) != executor_pid and
        Task.Supervisor.children(Handoff.DagTaskSupervisor) == []
    end)

    # The caller must not hang: its GenServer.call exits with the executor.
    assert {:ok, {:caller_exited, _reason}} =
             Task.yield(slow_result, 5_000) || Task.shutdown(slow_result)
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition not met in time")

      true ->
        Process.sleep(50)
        wait_until(fun, attempts - 1)
    end
  end

  test "a crashing DAG does not kill a concurrently running DAG" do
    slow_result = Task.async(fn -> DistributedExecutor.execute(slow_dag(1_500)) end)

    # Give the slow DAG time to be dispatched before crashing its sibling.
    Process.sleep(200)

    assert {:error, {:execution_crashed, _reason}} =
             DistributedExecutor.execute(crashing_dag())

    assert {:ok, %{results: %{slow: :done}}} = Task.await(slow_result, 10_000)
  end
end
