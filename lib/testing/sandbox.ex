defmodule Polyn.Sandbox do
  @moduledoc """
  Sandbox environment for mocking NATS and keeping tests isolated

  Add the following to your test_helper.ex

  ```elixir
  Polyn.Sandbox.start_link()
  ```
  """

  use Agent

  def start_link(_initial_value) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def state do
    Agent.get(__MODULE__, & &1)
  end

  def get(test_pid) do
    Agent.get(__MODULE__, &Map.get(&1, test_pid))
  end

  def setup_test(test_pid, nats_pid) do
    Agent.update(__MODULE__, &Map.put(&1, test_pid, nats_pid))
  end

  def teardown_test(test_pid) do
    Agent.update(__MODULE__, &Map.delete(&1, test_pid))
  end
end
