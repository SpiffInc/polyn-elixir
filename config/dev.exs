import Config

config :polyn, :domain, "com.acme"
config :polyn, :source_root, "user.backend"

config :polyn, :nats, %{
  name: :gnat,
  connection_settings: [
    %{host: "localhost", port: 4222}
  ]
}
