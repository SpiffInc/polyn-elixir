defmodule Polyn.Connection do
  # Utilities to help work with server connection
  @moduledoc false

  @doc """
  Get the name of the NATS server connection
  """
  def name do
    config().name
  end

  defp config do
    Application.fetch_env!(:polyn, :nats)
  end
end
