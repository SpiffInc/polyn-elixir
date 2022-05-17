import Config

config :polyn, :domain, "com.acme"
config :polyn, :source_root, "my_app"

config :polyn, :nats, %{
  name: :gnat,
  connection_settings: [
    %{host: "localhost", port: 4222}
  ]
}
