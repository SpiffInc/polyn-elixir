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
  `datacontenttype` - Content type of the data value. Must adhere to RFC 2046 format.
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
          datacontenttype: String.t(),
          source: String.t(),
          time: String.t(),
          polyntrace: list(map()),
          polynclient: map()
        }

  @doc """
  Create a new `Polyn.Event`
  """
  @spec new(fields :: keyword()) :: t()
  def new(fields) when is_list(fields) do
    fields =
      Keyword.put_new(fields, :id, UUID.uuid4())
      |> Keyword.put_new(:time, DateTime.to_iso8601(DateTime.utc_now()))

    struct!(__MODULE__, fields)
  end

  @spec new(fields :: map()) :: t()
  def new(fields) when is_map(fields) do
    Enum.into(fields, Keyword.new()) |> new()
  end

  @doc """
  Build an Event `type` field. Will automatically prefix your application's
  reverse-DNS name and handle version syntax

  ## Examples

      iex>Polyn.Event.type("user.created")
      "com.my_app.user.created.v1"

      iex>Polyn.Event.type("user.created", version: 2)
      "com.my_app.user.created.v2"
  """
  @spec type(type :: binary(), opts :: keyword()) :: binary()
  def type(type, opts \\ []) do
    version = Keyword.get(opts, :version, 1)
    "#{domain()}.#{type}.v#{version}"
  end

  @doc """
  Build an Event `dataschema` [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier)

  ## Examples

      iex>Polyn.Event.Type("user.created") |> Polyn.Event.dataschema()
      "com:my_app:user:created:v1:schema:v1"

      iex>Polyn.Event.Type("user.created", version: 2) |> Polyn.Event.dataschema(version: 2)
      "com:my_app:user:created:v2:schema:v2"
  """
  @spec dataschema(event_type :: binary(), opts :: keyword()) :: binary()
  def dataschema(event_type, opts \\ []) do
    version = Keyword.get(opts, :version, 1)
    event_type = String.replace(event_type, ".", ":")
    "#{event_type}:schema:v#{version}"
  end

  defp domain do
    Application.fetch_env!(:polyn, :domain)
  end
end
