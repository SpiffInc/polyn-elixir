defmodule Polyn.Nats do
  @moduledoc false
  # The real nats for production

  @behaviour Polyn.NatsBehaviour

  @impl Polyn.NatsBehaviour
  def pub(conn, subject, data, opts \\ []) do
    Gnat.pub(conn, subject, data, opts)
  end

  @impl Polyn.NatsBehaviour
  def sub(conn, subject, opts \\ []) do
    Gnat.sub(conn, subject, opts)
  end

  @impl Polyn.NatsBehaviour
  def request(conn, subject, data, opts \\ []) do
    Gnat.request(conn, subject, data, opts)
  end
end
