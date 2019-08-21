defmodule AvroExV0.Mixfile do
  use Mix.Project

  def project do
    [
      app: :avro_ex_v0,
      version: "0.1.0-beta.6",
      elixir: "~> 1.6",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      package: package(),
      description: "Old version of AvroEx for legacy support",
      deps: deps()
    ]
  end

  def application do
    # Specify extra applications you'll use from Erlang/Elixir
    [extra_applications: [:logger, :ecto]]
  end

  defp aliases do
    [compile: ["compile --warnings-as-errors"]]
  end

  defp deps do
    [
      {:poison, "~> 3.1.0"},
      {:ex_doc, "~> 0.18.0", only: :dev, runtime: false},
      {:ecto, "~> 2.1.0 or ~> 2.2.0"}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      maintainers: ["grant.mclendon@7mind.de"],
      links: %{"Github" => "http://github.com/gerbal/avro_ex/tree/v0"}
    ]
  end
end
