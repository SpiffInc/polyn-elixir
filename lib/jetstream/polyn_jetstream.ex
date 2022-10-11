defmodule Polyn.Jetstream do
  @moduledoc false

  alias Jetstream.API.{Consumer, Stream}

  defdelegate list_streams(conn, params \\ []), to: Stream, as: :list

  @doc """
  Get info for a stream or raise if it doesn't exist
  """
  @spec stream_info!(conn :: Gnat.t(), stream_name :: binary()) :: Stream.info()
  def stream_info!(conn, stream_name) do
    case Stream.info(conn, stream_name) do
      {:ok, info} ->
        info

      {:error, error} ->
        raise Polyn.StreamException,
              "Could not find a stream named #{stream_name}. #{inspect(error)}"
    end
  end

  @doc """
  Get info for a consumer or raise if it doesn't exist
  """
  @spec consumer_info!(conn :: Gnat.t(), stream_name :: binary(), consumer_name :: binary()) ::
          Consumer.info()
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
  Get a list of subjects a consumer cares about
  """
  @spec subjects_for_consumer(
          conn :: Gnat.t(),
          stream_name :: binary(),
          consumer_name :: binary()
        ) :: [binary()]
  def subjects_for_consumer(conn, stream_name, consumer_name) do
    stream = stream_info!(conn, stream_name)
    consumer = consumer_info!(conn, stream_name, consumer_name)
    find_consumer_subjects(stream, consumer)
  end

  defp find_consumer_subjects(stream, consumer) do
    stream_subjects = stream.config.subjects
    consumer_subject = consumer.config.filter_subject

    case consumer_subject do
      nil -> stream_subjects
      subject -> [subject]
    end
  end
end
