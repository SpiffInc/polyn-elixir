defmodule Polyn.Migration do
  @moduledoc """
  Functions for making changes to a NATS server
  """
  @callback change() :: nil

  alias Jetstream.API.Stream

  @spec create_stream(stream_options :: keyword()) :: {:ok, Stream.info()} | {:error, any()}
  def create_stream(options) do
    stream = struct!(Stream, options)

    connection_name()
    |> Stream.create(stream)
  end

  @spec delete_stream(stream_name :: binary()) :: :ok | {:error, any()}
  def delete_stream(stream_name) do
    connection_name()
    |> Stream.delete(stream_name)
  end

  defp connection_name do
    connection_config().name
  end

  defp connection_config do
    Application.fetch_env!(:polyn, :nats)
  end
end
