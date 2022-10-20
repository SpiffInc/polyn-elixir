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
    block = record_exceptions(block)

    quote do
      OpenTelemetry.Tracer.with_span("#{unquote(type)} send", %{kind: "PRODUCER"},
        do: unquote(block)
      )
    end
  end

  @doc """
  Start a span for handling a received message for an event
  """
  defmacro subscribe_span(type, headers, do: block) do
    block = record_exceptions(block)

    quote do
      # Extract a `traceparent` header from a message so it can connect the current span to a remote span
      # https://www.w3.org/TR/trace-context/#traceparent-header
      if unquote(headers) do
        :otel_propagator_text_map.extract(unquote(headers))
      end

      OpenTelemetry.Tracer.with_span("#{unquote(type)} receive", %{kind: "CONSUMER"},
        do: unquote(block)
      )
    end
  end

  @doc """
  Start a span to handle processing of batch messages
  """
  defmacro processing_span(type, do: block) do
    block = record_exceptions(block)

    quote do
      OpenTelemetry.Tracer.with_span("#{unquote(type)} process", %{kind: "CONSUMER"},
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

  @doc """
  Add a `traceparent` header to the headers for a message so that the
  subscribers can be connected with it
  https://www.w3.org/TR/trace-context/#traceparent-header
  """
  def add_trace_header(headers) do
    :otel_propagator_text_map.inject(headers)
  end

  @doc """
  Link one span to another span
  """
  def link_to_context(ctx) do
    OpenTelemetry.link(ctx)
  end

  @doc """
  Get the current span context
  """
  def current_context do
    OpenTelemetry.Ctx.get_current()
  end

  # Any errors that happen, expecially validation errors, we want the span to record so observability tools
  # will show the error
  defp record_exceptions(block) do
    quote do
      try do
        unquote(block)
      rescue
        e ->
          OpenTelemetry.Tracer.record_exception(e, __STACKTRACE__)
          reraise e, __STACKTRACE__
      end
    end
  end

  @doc """
  Add non-raised timeout exceptions to the current span
  """
  def record_timeout_exception(event_type, json) do
    OpenTelemetry.Tracer.record_exception(%RuntimeError{
      message: "request for #{event_type} timeout. payload: \n#{json}"
    })
  end
end
