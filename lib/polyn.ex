defmodule Polyn do
  @moduledoc """
  Polyn is a dead simple service framework designed to be language agnostic while
  providing a simple, yet powerful, abstraction layer for building reactive events
  based services.
  """

  alias Polyn.Event
  alias Polyn.Serializers.JSON

  @typedoc """
  Options you can pass to most `Polyn` module functions

  * `:source` - The `source` of the event. By default will be the `domain` combined with the
  `source_root`
  * `:triggered_by` - The event that triggered this one. Will use information from the event to build
  up the `polyntrace` data
  """
  @type polyn_options ::
          {:store_name, binary()}
          | {:source, binary()}
          | {:triggered_by, Event.t()}

  @typedoc """
  Options for publishing events. See `Gnat.pub/4` for more info

  * `:headers` - Headers to include in the message
  * `:reply_to` - Subject to send a response to
  """
  @type pub_options ::
          polyn_options()
          | {:headers, Gnat.headers()}
          | {:reply_to, binary()}

  @typedoc """
  Options for publishing events. See `Gnat.request/4` for more info

  * `:headers` - Headers to include in the message
  * `:receive_timeout` - How long to wait for a response
  """
  @type req_options ::
          polyn_options()
          | {:headers, Gnat.headers()}
          | {:receive_timeout, non_neg_integer()}

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
    event = build_event(conn, event_type, data, opts)

    Gnat.pub(conn, event_type, event, remove_polyn_opts(opts))
  end

  @doc """
  Issue a request in a psuedo-synchronous fashion. Requests still require an event be defined in
  the schema store. The event you send and receive will both be validated

  ## Options

  * `:source` - The `source` of the event. By default will be the `domain` combined with the
  `source_root`
  * `:triggered_by` - The event that triggered this one. Will use information from the event to build
  up the `polyntrace` data
  * See `Gnat.request/4` for other options

  ## Examples

      iex>Polyn.request(:gnat, "user.created.v1", %{name: "Mary"})
      {:ok, %{body: %Event{}}}
      iex>Polyn.request(:gnat, "user.created.v1", %{name: "Mary"}, source: "admin")
      {:ok, %{body: %Event{}}}
  """
  @spec request(
          conn :: Gnat.t(),
          event_type :: binary(),
          data :: any(),
          opts :: list(req_options())
        ) :: {:ok, Gnat.message()} | {:error, :timeout}
  def request(conn, event_type, data, opts \\ []) do
    event = build_event(conn, event_type, data, opts)

    case Gnat.request(conn, event_type, event, remove_polyn_opts(opts)) do
      {:ok, message} ->
        {:ok, Map.put(message, :body, JSON.deserialize!(message.body, conn, opts))}

      error ->
        error
    end
  end

  @doc """
  Reply to an event you've subscribed to that included a `reply_to` option.

  ## Options

  * `:source` - The `source` of the event. By default will be the `domain` combined with the
  `source_root`
  * `:triggered_by` - The event that triggered this one. Will use information from the event to build
  up the `polyntrace` data
  * See `Gnat.pub/4` for other options

  ## Examples

      iex>Polyn.reply(:gnat, "INBOX.me", "user.created.v1", %{name: "Mary"})
      :ok
      iex>Polyn.reply(:gnat, "INBOX.me", "user.created.v1", %{name: "Mary"}, source: "admin")
      :ok
  """
  @spec reply(
          conn :: Gnat.t(),
          reply_to :: binary(),
          event_type :: binary(),
          data :: any(),
          opts :: list(pub_options())
        ) ::
          :ok
  def reply(conn, reply_to, event_type, data, opts \\ []) do
    event = build_event(conn, event_type, data, opts)

    Gnat.pub(conn, reply_to, event, remove_polyn_opts(opts))
  end

  defp build_event(conn, event_type, data, opts) do
    Event.new(
      type: Event.full_type(event_type),
      data: data,
      specversion: "1.0.1",
      source: Event.full_source(Keyword.get(opts, :source)),
      datacontenttype: "application/json",
      polyntrace: build_polyntrace(Keyword.get(opts, :triggered_by))
    )
    |> JSON.serialize!(conn, opts)
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

  defp remove_polyn_opts(opts) do
    Keyword.drop(opts, [:source, :triggered_by, :store_name])
  end
end
