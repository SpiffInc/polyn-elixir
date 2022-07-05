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

  ## Examples

      iex>Polyn.pub(:gnat, "user.created.v1", %{name: "Mary"})
      :ok
      iex>Polyn.pub(:gnat, "user.created.v1", %{name: "Mary"}, source: "admin")
      :ok
  """
  @spec pub(conn :: Gnat.t(), event_type :: binary(), data :: any(), opts :: list(pub_options())) ::
          :ok
  def pub(conn, event_type, data, opts \\ []) do
    event =
      Event.new(
        type: Event.full_type(event_type),
        data: data,
        specversion: "1.0.1",
        source: Event.full_source(Keyword.get(opts, :source)),
        datacontenttype: "application/json",
        polyntrace: build_polyntrace(Keyword.get(opts, :triggered_by))
      )
      |> JSON.serialize!(conn, opts)

    Gnat.pub(conn, event_type, event)
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
