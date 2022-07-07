defmodule Polyn do
  @moduledoc """
  Polyn is a dead simple service framework designed to be language agnostic while
  providing a simple, yet powerful, abstraction layer for building reactive events
  based services.
  """

  alias Polyn.Event
  alias Polyn.Serializers.JSON

  @type pub_options :: {:store_name, binary()} | {:source, binary()} | {:triggered_by, Event.t()}

  @doc """
  Publish an event to the message bus. Will validate the data against an existing schema
  added by Polyn CLI.

  ## Options

  * `:source` - The `source` of the event. By default will be the `domain` combined with the
  `source_root`
  * `:triggered_by` - The event that triggered this one. Will use information from the event to build
  up the `polyntrace` data
  * See `Gnat.pub/4` for other options

  ## Examples

      iex>Polyn.pub(:gnat, "user.created.v1", %{name: "Mary"})
      :ok
      iex>Polyn.pub(:gnat, "user.created.v1", %{name: "Mary"}, source: "admin")
      :ok
  """
  @spec pub(conn :: Gnat.t(), event_type :: binary(), data :: any(), opts :: list(pub_options())) ::
          :ok
  def pub(conn, event_type, data, opts \\ []) do
    {source, opts} = Keyword.pop(opts, :source)
    {triggered_by, opts} = Keyword.pop(opts, :triggered_by)
    {store_name, opts} = Keyword.pop(opts, :store_name)

    event =
      Event.new(
        type: Event.full_type(event_type),
        data: data,
        specversion: "1.0.1",
        source: Event.full_source(source),
        datacontenttype: "application/json",
        polyntrace: build_polyntrace(triggered_by)
      )
      |> JSON.serialize!(conn, store_name: store_name)

    Gnat.pub(conn, event_type, event, opts)
  end

  defp build_polyntrace(nil), do: []

  defp build_polyntrace(%Event{} = triggered_by) do
    Enum.concat(triggered_by.polyntrace, [
      %{
        id: triggered_by.id,
        type: triggered_by.type,
        time: triggered_by.time
      }
    ])
  end
end
