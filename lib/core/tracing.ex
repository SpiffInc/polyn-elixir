defmodule Polyn.Tracing do
  # Functions to enable distributed tracing across services
  # Attempts to follow OpenTelemetry conventions
  # https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/messaging/
  @moduledoc false

  require OpenTelemetry.Tracer, as: Tracer

  @doc """
  Start a span for publishing an event
  """
  defmacro publish_span(type, do: expression) do
    quote do
      Tracer.with_span "#{unquote(type)} send", kind: "PRODUCER" do
        unquote(expression)
      end
    end
  end
end
