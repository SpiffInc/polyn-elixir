defmodule Polyn.Event do
  @moduledoc """
  The Event structure used throughout Polyn.
  """

  alias Polyn.Naming

  defstruct id: nil,
            specversion: nil,
            type: nil,
            data: nil,
            dataschema: nil,
            datacontenttype: nil,
            source: nil,
            time: nil,
            polyndata: %{
              clientlang: "elixir",
              clientlangversion: System.build_info().version
            }

  @typedoc """
  The Event structure used throughout Polyn.

  * `id` - Identifies the event.
  * `specversion` - The version of the CloudEvents specification which the event uses.
  * `type` - Describes the type of event related to the originating occurrence.
  * `data` - The event payload.
  * `dataschema` - Identifies the schema that data adheres to.
  * `datacontenttype` - Content type of the data value. Must adhere to RFC 2046 format.
  * `source` - Identifies the context in which an event happened.
  * `time` - Timestamp of when the occurrence happened. Must adhere to RFC 3339.
  * `polyndata` - Information about the client that produced the event and additional data
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
          polyndata: map()
        }

  @doc """
  Create a new `Polyn.Event`
  """
  @spec new(fields :: keyword()) :: t()
  def new(fields) when is_list(fields) do
    fields =
      Keyword.put_new(fields, :id, new_event_id())
      |> Keyword.put_new(:time, new_timestamp())
      |> Keyword.put_new(:source, full_source())

    struct!(__MODULE__, fields)
    |> add_polyn_version()
  end

  @spec new(fields :: map()) :: t()
  def new(fields) when is_map(fields) do
    Enum.into(fields, Keyword.new()) |> new()
  end

  @doc """
  Generate a new event id
  """
  def new_event_id do
    UUID.uuid4()
  end

  @doc """
  Generate a new timestamp for the event
  """
  def new_timestamp do
    DateTime.to_iso8601(DateTime.utc_now())
  end

  # we don't need to add both the atom and string versions
  defp add_polyn_version(%__MODULE__{polyndata: %{"clientversion" => _version}} = event) do
    event
  end

  defp add_polyn_version(%__MODULE__{} = event) do
    put_in(event, [Access.key!(:polyndata), :clientversion], polyn_version())
  end

  defp polyn_version do
    # Interporalating cuz `vsn` comes out as charlist instead of String
    "#{Application.spec(:polyn, :vsn)}"
  end

  @doc """
  Get the Event `source` prefixed with reverse domain name
  """
  @spec full_source(source :: binary() | nil) :: binary()
  @spec full_source() :: binary()
  def full_source(nil), do: full_source()

  def full_source(source) do
    Naming.validate_source_name!(source)
    source = String.replace(source, ~r/#{full_source()}{1}:?/, "")

    case source do
      "" -> full_source()
      name -> "#{full_source()}:#{Naming.dot_to_colon(name)}"
    end
  end

  def full_source do
    Naming.validate_source_name!(source_root())
    Naming.dot_to_colon("#{domain()}:#{source_root()}")
  end

  @doc """
  Get the Event `type` prefixed with reverse domain name
  """
  @spec full_type(type :: binary()) :: binary()
  def full_type(type) do
    Naming.validate_event_type!(type)
    "#{domain()}.#{Naming.trim_domain_prefix(type)}"
  end

  # The `domain` that all events will happen under
  defp domain do
    Application.fetch_env!(:polyn, :domain)
  end

  defp source_root do
    Application.fetch_env!(:polyn, :source_root)
  end
end
