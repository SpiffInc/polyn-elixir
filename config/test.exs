import Config

config :polyn, :file, Polyn.FileMock
config :polyn, :code, Polyn.CodeMock

config :polyn, :domain, "com.test"

config :polyn, :nats, %{
  name: :test_gnat,
  connection_settings: [
    %{host: "localhost", port: 4222}
  ]
}
