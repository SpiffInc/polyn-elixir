defmodule Polyn.Producer do
  @moduledoc """
  Use `Polyn.Producer` to publish new events to the NATS server
  """

  alias Polyn.Connection
  alias Polyn.Event

  def pub(event_type, source, data, opts \\ []) do
    event =
      Event.new(
        type: Event.full_type(event_type),
        data: data,
        specversion: "1.0.1",
        source: Event.full_source(source),
        datacontenttype: "application/json"
      )

    Gnat.pub(Connection.name(), event_type, data)
  end
end
