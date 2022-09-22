defmodule Polyn.Jetstream.MockAcknowledger do
  # Mock calls to Offbroadway.Jetstream.Acknowledger for testing isolation
  @moduledoc false

  @behaviour Broadway.Acknowledger

  @impl Broadway.Acknowledger
  def ack(_ack_ref, _successful, _failed) do
    :ok
  end

  @impl Broadway.Acknowledger
  def configure(_ack_ref, ack_data, _options) do
    {:ok, ack_data}
  end
end
