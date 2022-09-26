defmodule Polyn.Nats do
  # The real nats for production
  # This exists to ensure we don't mock things we don't "own"
  @moduledoc false

  @behaviour Polyn.NatsBehaviour

  @impl Polyn.NatsBehaviour
  def pub(conn, subject, data, opts \\ []) do
    Gnat.pub(conn, subject, data, opts)
  end

  @impl Polyn.NatsBehaviour
  def sub(conn, subscriber, subject, opts \\ []) do
    Gnat.sub(conn, subscriber, subject, opts)
  end

  @impl Polyn.NatsBehaviour
  def request(conn, subject, data, opts \\ []) do
    Gnat.request(conn, subject, data, opts)
  end

  @impl Polyn.NatsBehaviour
  def unsub(conn, sid, opts \\ []) do
    Gnat.unsub(conn, sid, opts)
  end
end
