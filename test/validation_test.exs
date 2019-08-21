defmodule AvroExV0.Validation.Test do
  use ExUnit.Case

  import AvroExV0.Error
  alias AvroExV0.Schema.Record
  alias Ecto.Changeset

  @test_module AvroExV0.Validation

  describe "validate_string" do
    cs =
      %Record{}
      |> Changeset.cast(%{"name" => :abc}, [:name])
      |> @test_module.validate_string(:name)

    assert error("must be a string") in errors(cs, :name)
  end
end
