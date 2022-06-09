defmodule Polyn.Producer do
  @moduledoc """
  Use `Polyn.Producer` to publish new events to the NATS server
  """

  alias Polyn.Connection
  alias Polyn.Event
  alias Polyn.Serializers.JSON

  @type pub_options :: {:store_name, binary()} | {:source, binary()}

  @doc """
  Publish an event to the message bus. Will validate the data against an existing schema
  added by Polyn CLI.

  ## Options

  * `:source` - The `source` of the event. By default will be the `domain` combined with the
  `source_root`
  * `:triggered_by` - The event that triggered this one. Will use information from the event to build
  up the `polyntrace` data

  ## Examples

      iex>Polyn.Producer.pub("user.created.v1", %{name: "Mary"})
      :ok
      iex>Polyn.Producer.pub("user.created.v1", %{name: "Mary"}, source: "admin")
      :ok
  """
  @spec pub(event_type :: binary(), data :: any(), opts :: list(pub_options())) :: :ok
  def pub(event_type, data, opts \\ []) do
    event =
      Event.new(
        type: Event.full_type(event_type),
        data: data,
        specversion: "1.0.1",
        source: Event.full_source(Keyword.get(opts, :source)),
        datacontenttype: "application/json",
        polyntrace: build_polyntrace(Keyword.get(opts, :triggered_by))
      )
      |> JSON.serialize(opts)

    Gnat.pub(Connection.name(), event_type, event)
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
