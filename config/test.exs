import Config

config :polyn, :file, Polyn.FileMock

config :polyn, :nats, %{
  name: :test_gnat,
  connection_settings: [
    %{host: "localhost", port: 4222}
  ]
}
