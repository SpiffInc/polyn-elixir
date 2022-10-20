defmodule Polyn do
  @moduledoc """
  Polyn is a dead simple service framework designed to be language agnostic while
  providing a simple, yet powerful, abstraction layer for building reactive events
  based services.
  """

  use Polyn.Tracing

  alias Polyn.Event
  alias Polyn.Serializers.JSON

  @typedoc """
  Options you can pass to most `Polyn` module functions

  * `:source` - The `source` of the event. By default will be the `domain` combined with the
  `source_root`
  """
  @type polyn_options ::
          {:store_name, binary()}
          | {:source, binary()}

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
    Polyn.Tracing.publish_span event_type do
      event = build_event(event_type, data, opts)

      json = JSON.serialize!(event, opts)

      Polyn.Tracing.span_attributes(conn: conn, type: event_type, event: event, payload: json)

      opts =
        add_nats_msg_id_header(opts, event)
        |> inject_trace_header()

      nats().pub(conn, event_type, json, remove_polyn_opts(opts))
    end
  end

  @doc """
  Issue a request in a psuedo-synchronous fashion. Requests still require an event be defined in
  the schema store. The event you send and receive will both be validated

  ## Options

  * `:source` - The `source` of the event. By default will be the `domain` combined with the
  `source_root`
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
    Polyn.Tracing.publish_span event_type do
      event = build_event(event_type, data, opts)

      opts =
        add_nats_msg_id_header(opts, event)
        |> inject_trace_header()

      json = JSON.serialize!(event, opts)

      Polyn.Tracing.span_attributes(conn: conn, type: event_type, event: event, payload: json)

      case nats().request(
             conn,
             event_type,
             json,
             remove_polyn_opts(opts)
           ) do
        {:ok, message} ->
          handle_reponse_success(conn, message, opts)

        error ->
          Polyn.Tracing.record_timeout_exception(event_type, json)
          error
      end
    end
  end

  defp handle_reponse_success(conn, message, opts) do
    # The :reply_to subject is a temporarily generated "inbox"
    # https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/messaging/#span-name
    Polyn.Tracing.subscribe_span "(temporary)", message.headers do
      event = JSON.deserialize!(message.body, opts)

      Polyn.Tracing.span_attributes(
        conn: conn,
        type: "(temporary)",
        event: event,
        payload: message.body
      )

      {:ok, Map.put(message, :body, event)}
    end
  end

  @doc """
  Reply to an event you've subscribed to that included a `reply_to` option.

  ## Options

  * `:source` - The `source` of the event. By default will be the `domain` combined with the
  `source_root`
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
    Polyn.Tracing.publish_span "(temporary)" do
      event = build_event(event_type, data, opts)

      json = JSON.serialize!(event, opts)

      Polyn.Tracing.span_attributes(
        conn: conn,
        type: "(temporary)",
        event: event,
        payload: json
      )

      opts =
        add_nats_msg_id_header(opts, event)
        |> inject_trace_header()

      nats().pub(conn, reply_to, json, remove_polyn_opts(opts))
    end
  end

  defp build_event(event_type, data, opts) do
    Event.new(
      type: Event.full_type(event_type),
      data: data,
      specversion: "1.0.1",
      source: Event.full_source(Keyword.get(opts, :source)),
      datacontenttype: "application/json"
    )
  end

  defp remove_polyn_opts(opts) do
    Keyword.drop(opts, [:source, :store_name])
  end

  defp add_nats_msg_id_header(opts, event) do
    # Ensure accidental message duplication doesn't happen
    # https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive#message-deduplication
    headers =
      Keyword.get(opts, :headers, [])
      |> Enum.concat([{"Nats-Msg-Id", event.id}])

    Keyword.put(opts, :headers, headers)
  end

  defp inject_trace_header(opts) do
    headers = Polyn.Tracing.add_trace_header(opts[:headers])
    Keyword.put(opts, :headers, headers)
  end

  defp nats do
    if sandbox(), do: Polyn.MockNats, else: Gnat
  end

  defp sandbox do
    Application.get_env(:polyn, :sandbox, false)
  end
end
