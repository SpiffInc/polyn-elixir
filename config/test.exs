import Config

config :polyn, :file, Polyn.FileMock

config :polyn, :domain, "com.test"
config :polyn, :source_root, "user.backend"

config :polyn, :nats, %{
  name: :test_gnat,
  connection_settings: [
    %{host: "localhost", port: 4222}
  ]
}
