import Config

config :polyn, :domain, "com.acme"

config :polyn, :nats, %{
  name: :gnat,
  connection_settings: [
    %{host: "localhost", port: 4222}
  ]
}
