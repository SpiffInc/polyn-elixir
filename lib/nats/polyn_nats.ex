defmodule Polyn.Nats do
  # The real nats for production
  # This exists to ensure we don't mock things we don't "own"
  @moduledoc false

  @behaviour Polyn.NatsBehaviour

  defdelegate pub(conn, subject, data, opts \\ []), to: Gnat
  defdelegate sub(conn, subscriber, subject, opts \\ []), to: Gnat
  defdelegate unsub(conn, sid, opts \\ []), to: Gnat
  defdelegate request(conn, subject, data, opts \\ []), to: Gnat
end
