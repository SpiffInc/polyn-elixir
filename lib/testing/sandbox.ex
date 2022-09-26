defmodule Polyn.Sandbox do
  @moduledoc """
  Sandbox environment for mocking NATS and keeping tests isolated

  Add the following to your test_helper.ex

  ```elixir
  Polyn.Sandbox.start_link()
  ```

  ## Nested Processes

  `Polyn.Testing` associates each test process with its own NATS mock.
  To allow other processes that will call `Polyn` functions to use the same
  NATS mock as the rest of the test use the `Polyn.Sandbox.allow/2` function.
  If you don't have access to the `pid` or name of a process that is using `Polyn`
  you will need to make your file `async: false`.
  """

  use Agent

  @doc """
  Start the Sandbox
  """
  @spec start_link(any()) :: Agent.on_start()
  def start_link(_initial_value) do
    Agent.start_link(fn -> %{async: false, pids: %{}} end, name: __MODULE__)
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
  @spec get!(pid()) :: pid()
  def get!(pid) do
    result = Agent.get(__MODULE__, &lookup_nats(&1, pid))

    case result do
      nil ->
        raise Polyn.TestingException, no_nats_server_msg(pid)

      nats_pid ->
        nats_pid
    end
  end

  # When async: false we're assuming only 1 test is running
  # and only one association should exist
  defp lookup_nats(%{async: false, pids: pids}, _pid) when map_size(pids) == 1 do
    Map.values(pids) |> Enum.at(0) |> Map.get(:nats)
  end

  defp lookup_nats(%{pids: pids}, pid) do
    get_in(pids, [pid, :nats])
  end

  @doc """
  Get the async mode of the Sandbox. Defaults to false
  """
  @spec get_async_mode() :: boolean()
  def get_async_mode do
    Agent.get(__MODULE__, &Map.get(&1, :async, false))
  end

  @doc """
  Setup a test with a mock nats server association
  """
  @spec setup_test(test_pid :: pid(), nats_pid :: pid()) :: :ok
  def setup_test(test_pid, nats_pid) do
    Agent.update(__MODULE__, &put_in(&1, [:pids, test_pid], %{nats: nats_pid}))
  end

  @doc """
  Remove the nats server assocation when a test is finished
  """
  @spec teardown_test(test_pid :: pid()) :: :ok
  def teardown_test(test_pid) do
    Agent.update(__MODULE__, fn state ->
      pids =
        Map.delete(state.pids, test_pid)
        |> remove_allowed_pids(test_pid)

      Map.put(state, :pids, pids)
    end)
  end

  defp remove_allowed_pids(pids, test_pid) do
    Enum.reduce(pids, %{}, fn {key, value}, acc ->
      if value[:allowed_by] == test_pid do
        acc
      else
        Map.put(acc, key, value)
      end
    end)
  end

  @doc """
  Make the sandbox async false or true
  """
  @spec set_async_mode(mode :: boolean()) :: :ok
  def set_async_mode(mode) do
    Agent.update(__MODULE__, &Map.put(&1, :async, mode))
  end

  @doc """
  Allow a child process, that is not the test process, to access the running
  MockNats server. You cannot allow the same process on multiple tests.

  ## Examples

      iex>Polyn.Sandbox.allow(self(), Process.whereis(:foo))
      :ok
  """
  @spec allow(test_pid :: pid(), other_pid :: pid()) :: :ok
  def allow(test_pid, other_pid) do
    Agent.update(__MODULE__, fn state ->
      validate_allowance!(state.pids, other_pid)
      mock_nats = state.pids[test_pid][:nats]
      put_in(state, [:pids, other_pid], %{nats: mock_nats, allowed_by: test_pid})
    end)
  end

  defp validate_allowance!(test_pids, pid) do
    case test_pids[pid] do
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
    as the NATS state will not be isolated. Instead, refactor code so that the process
    is not shared between tests or make these tests `async: false`
    """
  end

  defp no_nats_server_msg(pid) do
    """
    \nTo keep NATS data isolated in concurrently running tests each
    test needs its own MockNats Server. There are no MockNats servers
    associated with process #{inspect(pid)}. This could happen
    for several reasons:

    1. Did you forget to add
    ```
    import Polyn.Testing
    setup :setup_polyn
    ````
    to the top of your test file?

    2. Is your call to `Polyn` happening in a Process other than the
    test process? If so you'll need to explicitly associate that process
    by using `Polyn.Sandbox.allow/2`

    3. If your `Polyn` calls are happening in a Process that isn't
    accessible to you, you'll need to make your test `async: false`
    """
  end
end
