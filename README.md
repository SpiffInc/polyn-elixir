# Polyn

Polyn is a dead simple service framework designed to be language agnostic while
providing a simple, yet powerful, abstraction layer for building reactive events
based services.

## Philosophy

According to [Jonas Boner](http://jonasboner.com/), reactive Microservices require
you to:
1. Follow the principle “do one thing, and one thing well” in defining service
   boundaries
2. Isolate the services
3. Ensure services act autonomously
4. Embrace asynchronous message passing
5. Stay mobile, but addressable
6. Design for the required level of consistency

Polyn implements this pattern in a manner that can be applied to multiple programming
languages, such as Ruby, Elixir, or Python, enabling you to build services that can
communicate regardless of the language you use.

Using an event-based microservice architecture is a great way to decouple your services,
create reliability, and scalability. However, there is no standard way to format events
which creates entropy and inconsistency between services, requiring developers to
create different event handling logic for each event type they consume. Polyn
solves this problem by creating and enforcing a consistent event format on both the
producer and consumer-side so all the services in your system can focus their
effort on the data rather than the event format.

Rather than defining its own event schema, Polyn uses the [Cloud Events](https://github.com/cloudevents/spec)
specification and strictly enforces the event format. This means that you can use Polyn to build services
that can be used by other services, or natively interact with things such as GCP Cloud Functions.

For events that include `data` Polyn also leverages the [JSON Schema](http://json-schema.org/)
specification to create consistency.

## Configuration

### Connection

You will need to provide the connection settings for your NATS connection. This will differ in-between environments.

```elixir
config :polyn, :nats, %{
  name: :gnat, # (required) the registered named you want to give the Gnat connection
  backoff_period: 4_000, # number of milliseconds to wait between consecutive reconnect attempts (default: 2_000)
  connection_settings: [
    %{host: "10.0.0.100", port: 4222},
    %{host: "10.0.0.101", port: 4222},
  ]
}
```

## Event and Data Serialization

### `datacontenttype`

The [Cloud Event Spec](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#datacontenttype) allows for the possibility for the `data` in the event not be JSON. For example you may have an event that is being
serialized as JSON, but the data inside is XML. By default the serializer will assume any `data` JSON. If the `data` differs you must specify its format with the `datacontenttype` attribute.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `polyn` to your list of dependencies in `mix.exs` :

```elixir
def deps do
  [
    {:polyn, "~> 0.1.0"}
  ]
end
```

## Documentation

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/polyn](https://hexdocs.pm/polyn).
