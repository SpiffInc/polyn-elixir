defmodule Polyn.Sandbox do
  @moduledoc """
  Sandbox environment for mocking NATS and keeping tests isolated

  Add the following to your test_helper.ex

  ```elixir
  Polyn.Sandbox.start_link()
  ```
  """

  use Agent

  @doc """
  Start the Sandbox
  """
  @spec start_link(any()) :: Agent.on_start()
  def start_link(_initial_value) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc """
  Get the full state
  """
  @spec state() :: map()
  def state do
    Agent.get(__MODULE__, & &1)
  end

  @doc """
  Get the nats server for a given pid
  """
  @spec get(pid()) :: pid() | nil
  def get(pid) do
    Agent.get(__MODULE__, &get_in(&1, [pid, :nats]))
  end

  @doc """
  Setup a test with a mock nats server association
  """
  @spec setup_test(test_pid :: pid(), nats_pid :: pid()) :: :ok
  def setup_test(test_pid, nats_pid) do
    Agent.update(__MODULE__, &Map.put(&1, test_pid, %{nats: nats_pid}))
  end

  @doc """
  Remove the nats server assocation when a test is finished
  """
  @spec teardown_test(test_pid :: pid()) :: :ok
  def teardown_test(test_pid) do
    Agent.update(__MODULE__, &Map.delete(&1, test_pid))
  end

  @doc """
  Allow a child process, that is not the test process, to access the running
  MockNats server.

  ## Examples

      iex>Polyn.Sandbox.allow(self(), Process.whereis(:foo))
      :ok
  """
  @spec allow(test_pid :: pid(), other_pid :: pid()) :: :ok
  def allow(test_pid, other_pid) do
    Agent.update(__MODULE__, fn state ->
      validate_allowance!(state, other_pid)
      mock_nats = state[test_pid][:nats]
      Map.put(state, other_pid, %{nats: mock_nats, allowed_by: test_pid})
    end)
  end

  defp validate_allowance!(state, pid) do
    case state[pid] do
      nil ->
        :ok

      existing ->
        raise Polyn.TestingException, already_allowed_msg(pid, existing.allowed_by)
    end
  end

  defp already_allowed_msg(pid, allowed_by) do
    """
    \nYou tried to call `Polyn.Sandbox.allow/2` with a `pid` of #{inspect(pid)}
    that is already associated with a running test #{inspect(allowed_by)}. This is
    possibly because you have a shared process that has a lifecycle that
    spans multiple tests. This can cause tests to be flaky and have race conditions
    as the NATS state will not be isolated. Instead make these tests `async: false`
    """
  end
end
