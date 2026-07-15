defmodule Handoff.Supervisor do
  @moduledoc """
  Main supervisor for the Handoff execution engine.

  Manages the lifecycle of all components needed for DAG execution.
  """

  use Supervisor

  require Logger

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    resource_tracker =
      Keyword.get(opts, :resource_tracker) ||
        Application.get_env(:handoff, :resource_tracker, Handoff.SimpleResourceTracker)

    children = [
      {Handoff.ResultStore, []},
      resource_tracker,
      {Handoff.DataLocationRegistry, []},
      {Handoff.DistributedExecutor, Keyword.put(opts, :resource_tracker, resource_tracker)},
      # Runs DAG execution tasks unlinked from DistributedExecutor so a crash
      # in one DAG cannot take down the executor (and with it every other
      # in-flight DAG on the node). Placed AFTER the executor under
      # :rest_for_one so an executor restart also restarts this supervisor,
      # terminating in-flight DAG tasks — the restarted executor has no
      # record of them, and orphans would keep executing side effects with
      # nobody to reply to.
      {Task.Supervisor, name: Handoff.DagTaskSupervisor},
      {Handoff.DistributedResultStore, []}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
