defmodule Polyn.Producer do
  @moduledoc """
  Use `Polyn.Producer` to publish new events to the NATS server
  """

  alias Polyn.Connection
  alias Polyn.Event
  alias Polyn.Serializers.JSON

  def pub(event_type, data, opts \\ []) do
    event =
      Event.new(
        type: Event.full_type(event_type),
        data: data,
        specversion: "1.0.1",
        source: Event.full_source(Keyword.get(opts, :source)),
        datacontenttype: "application/json"
      )
      |> JSON.serialize(opts)

    Gnat.pub(Connection.name(), event_type, event)
  end
end
