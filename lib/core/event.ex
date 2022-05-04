defmodule Polyn.Event do
  @moduledoc """
  The Event structure used throughout Polyn.
  """
  defstruct id: nil,
            specversion: nil,
            type: nil,
            data: nil,
            dataschema: nil,
            datacontenttype: nil,
            source: nil,
            time: nil,
            polyntrace: [],
            polynclient: %{
              lang: "elixir",
              langversion: System.build_info().version,
              version: Polyn.MixProject.version()
            }

  @typedoc """
  `id` - Identifies the event.
  `specversion` - The version of the CloudEvents specification which the event uses.
  `type` - Describes the type of event related to the originating occurrence.
  `data` - The event payload.
  `dataschema` - Identifies the schema that data adheres to.
  `source` - Identifies the context in which an event happened.
  `time` - Timestamp of when the occurrence happened. Must adhere to RFC 3339.
  `polyntrace` - Previous events that led to this one
  `polynclient` - Information about the client that produced the event
  """
  @type t() :: %__MODULE__{
          id: String.t(),
          specversion: String.t(),
          type: String.t(),
          data: any(),
          dataschema: String.t(),
          source: String.t(),
          time: String.t(),
          polyntrace: list(map()),
          polynclient: map()
        }

  def new(fields) when is_list(fields) do
    fields =
      Keyword.put_new(fields, :id, UUID.uuid4())
      |> Keyword.put_new(:time, DateTime.to_iso8601(DateTime.utc_now()))

    struct!(__MODULE__, fields)
  end
end