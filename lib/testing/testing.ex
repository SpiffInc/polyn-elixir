defmodule Polyn.Testing do
  @moduledoc """
  Use this module to create isolated NATS environments for each test.
  Add the following to your test file

  ```elixir
  import Polyn.Testing

  setup :setup_polyn
  ```
  """

  import ExUnit.Callbacks
  alias Polyn.Sandbox

  @doc """
  Intended to be a ExUnit `setup` function that will create an isolated NATS environment
  for each test
  """
  def setup_polyn(context) do
    Sandbox.set_async_mode(context.async)

    mock_nats = start_supervised!(Polyn.MockNats)
    Sandbox.setup_test(self(), mock_nats)

    on_exit(fn ->
      Sandbox.teardown_test(self())
    end)

    context
  end
end
