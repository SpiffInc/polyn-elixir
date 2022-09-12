defmodule Polyn.Nats do
  @moduledoc false
  # The real nats for production

  @behaviour Polyn.NatsBehaviour

  defdelegate pub(conn, subject, data, opts \\ []), to: Gnat
  defdelegate sub(conn, subscriber, subject, opts \\ []), to: Gnat
  defdelegate unsub(conn, sid, opts \\ []), to: Gnat
  defdelegate request(conn, subject, data, opts \\ []), to: Gnat
end
