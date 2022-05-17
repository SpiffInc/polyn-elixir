defmodule Polyn.Event do
  alias Polyn.Naming

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
              langversion: System.build_info().version
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
      |> Keyword.put_new(:source, source())

    struct!(__MODULE__, fields)
    |> add_polyn_version()
  end

  @spec new(fields :: map()) :: t()
  def new(fields) when is_map(fields) do
    Enum.into(fields, Keyword.new()) |> new()
  end

  defp add_polyn_version(%__MODULE__{} = event) do
    put_in(event, [Access.key!(:polynclient), :version], polyn_version())
  end

  defp polyn_version do
    # Interporalating cuz `vsn` comes out as charlist instead of String
    "#{Application.spec(:polyn, :vsn)}"
  end

  @doc """
  Build an Event `type` field. Will automatically prefix your application's
  reverse-DNS name and handle version syntax

  ## Examples

      # Given a `domain` of `com.my_app`
      iex>Polyn.Event.type("user.created")
      "com.my_app.user.created.v1"

      iex>Polyn.Event.type("user.created", version: 2)
      "com.my_app.user.created.v2"
  """
  @spec type(type :: binary(), opts :: keyword()) :: binary()
  def type(type, opts \\ []) do
    version = Keyword.get(opts, :version, 1)
    "#{domain()}.#{type}." |> Naming.version_suffix(version)
  end

  @doc """
  Updates the event to have a bare `type` field without domain or versioning

  ## Examples

      # Given a `domain` of `com.my_app`
      iex>Polyn.Event.with_bare_type(%Event{type: "com.my_app.user.created.v1"})
      %Event{type: "user.created"}
  """
  @spec with_bare_type(event :: t()) :: t()
  def with_bare_type(%{type: type} = event) do
    type = Naming.trim_domain_prefix(type) |> Naming.trim_version_suffix()
    Map.put(event, :type, type)
  end

  @doc """
  Build an Event `dataschema` [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier)

  ## Examples

      # Given a `domain` of `com.my_app`
      iex>Polyn.Event.type("user.created") |> Polyn.Event.dataschema()
      "com:my_app:user:created:v1:schema:v1"

      iex>Polyn.Event.type("user.created", version: 2) |> Polyn.Event.dataschema(version: 2)
      "com:my_app:user:created:v2:schema:v2"
  """
  @spec dataschema(event_type :: binary(), opts :: keyword()) :: binary()
  def dataschema(event_type, opts \\ []) do
    version = Keyword.get(opts, :version, 1)
    Naming.dot_to_colon("#{event_type}:schema:") |> Naming.version_suffix(version)
  end

  @doc """
  Build an Event `source` [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier)

  ## Examples

      # Given a `domain` of `com.my_app`
      # Given a `source_root` of `orders`
      iex>Polyn.Event.source("user_producer")
      "com:my_app:orders:user_producer"

      iex>Polyn.Event.source()
      "com:my_app:orders"
  """
  @spec source() :: binary()
  def source do
    Naming.dot_to_colon("#{domain()}:#{source_root()}")
  end

  @spec source(name :: binary()) :: binary()
  def source(name) do
    "#{source()}:#{Naming.dot_to_colon(name)}"
  end

  # The `domain` that all events will happen under
  defp domain do
    Application.fetch_env!(:polyn, :domain)
  end

  defp source_root do
    Application.fetch_env!(:polyn, :source_root)
  end
end
