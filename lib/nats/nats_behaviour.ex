defmodule Polyn.NatsBehaviour do
  @moduledoc false
  # Module for defining a behaviour for nats interfaces
  @callback pub(conn :: Gnat.t(), subject :: binary(), data :: any(), opts :: keyword()) :: :ok
  @callback sub(conn :: Gnat.t(), subscriber :: pid(), subject :: binary(), opts :: keyword()) ::
              {:ok, non_neg_integer} | {:ok, String.t()} | {:error, String.t()}
  @callback request(conn :: Gnat.t(), subject :: binary(), data :: any(), opts :: keyword()) ::
              {:ok, Gnat.message()} | {:error, :timeout}
  @callback unsub(conn :: Gnat.t(), non_neg_integer() | String.t(), opts :: keyword()) :: :ok
end
