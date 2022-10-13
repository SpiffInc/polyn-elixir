defmodule Polyn.Tracing do
  # Functions to enable distributed tracing across services
  # Attempts to follow OpenTelemetry conventions
  # https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/messaging/
  @moduledoc false

  # Ensures calling module can access Tracer library
  defmacro __using__(_opts) do
    quote do
      require OpenTelemetry.Tracer
    end
  end

  @doc """
  Start a span for publishing an event
  """
  defmacro publish_span(type, do: block) do
    quote do
      OpenTelemetry.Tracer.with_span("#{unquote(type)} send", %{kind: "PRODUCER"},
        do: unquote(block)
      )
    end
  end

  @doc """
  Common attributes to add to a span involving an individual message
  https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/messaging/#messaging-attributesADd common at
  """
  defmacro span_attributes(conn: conn, type: type, event: event, payload: payload) do
    quote do
      OpenTelemetry.Tracer.set_attributes(%{
        "messaging.system" => "NATS",
        "messaging.destination" => unquote(type),
        "messaging.protocol" => "Polyn",
        "messaging.url" => Gnat.server_info(unquote(conn)).client_ip,
        "messaging.message_id" => unquote(event).id,
        "messaging.message_payload_size_bytes" => byte_size(unquote(payload))
      })
    end
  end

  ##
  # Common attributes to add to a span involving an individual message
  # https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/messaging/#messaging-attributes
  # def self.span_attributes(span, nats:, type:, event:, payload:)
  #   span.add_attributes({
  #     "messaging.system"                     => "NATS",
  #     "messaging.destination"                => type,
  #     "messaging.protocol"                   => "Polyn",
  #     "messaging.url"                        => nats.uri.to_s,
  #     "messaging.message_id"                 => event.id,
  #     "messaging.message_payload_size_bytes" => payload.bytesize,
  #   })
  # end
end
