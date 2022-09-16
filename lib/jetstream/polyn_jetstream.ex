defmodule Polyn.Jetstream do
  @moduledoc false

  alias Jetstream.API.{Consumer, Stream}

  def stream_info!(conn, stream_name) do
    case Stream.info(conn, stream_name) do
      {:ok, info} ->
        info

      {:error, error} ->
        raise Polyn.StreamException,
              "Could not find a stream named #{stream_name}. #{inspect(error)}"
    end
  end

  def consumer_info!(conn, stream_name, consumer_name) do
    case Consumer.info(conn, stream_name, consumer_name) do
      {:ok, info} ->
        info

      {:error, error} ->
        raise Polyn.StreamException,
              "Could not find a consumer named #{consumer_name} in stream #{stream_name}. #{inspect(error)}"
    end
  end

  @doc """
  Lookup the name of a stream for a given event type

  ## Examples

        iex>Polyn.Jetstream.lookup_stream_name!(:gnat, "user.created.v1")
        "USERS"

        iex>Polyn.Jetstream.lookup_stream_name!(:gnat, "foo.v1")
        Polyn.StreamException
  """
  def list_streams(conn, params \\ []) do
    Stream.list(conn, params)
  end
end
