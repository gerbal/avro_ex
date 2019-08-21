defmodule AvroExV0.Decode.Test do
  use ExUnit.Case

  @test_module AvroExV0.Decode

  describe "decode (primitive)" do
    test "null" do
      {:ok, schema} = AvroExV0.parse_schema(~S("null"))
      {:ok, avro_message} = AvroExV0.encode(schema, nil)
      assert {:ok, nil} = @test_module.decode(schema, avro_message)
    end

    test "boolean" do
      {:ok, schema} = AvroExV0.parse_schema(~S("boolean"))
      {:ok, true_message} = AvroExV0.encode(schema, true)
      {:ok, false_message} = AvroExV0.encode(schema, false)

      assert {:ok, true} = @test_module.decode(schema, true_message)
      assert {:ok, false} = @test_module.decode(schema, false_message)
    end

    test "integer" do
      {:ok, schema} = AvroExV0.parse_schema(~S("int"))
      {:ok, zero} = AvroExV0.encode(schema, 0)
      {:ok, neg_ten} = AvroExV0.encode(schema, -10)
      {:ok, ten} = AvroExV0.encode(schema, 10)
      {:ok, big} = AvroExV0.encode(schema, 5_000_000)
      {:ok, small} = AvroExV0.encode(schema, -5_000_000)

      assert {:ok, 0} = @test_module.decode(schema, zero)
      assert {:ok, -10} = @test_module.decode(schema, neg_ten)
      assert {:ok, 10} = @test_module.decode(schema, ten)
      assert {:ok, 5_000_000} = @test_module.decode(schema, big)
      assert {:ok, -5_000_000} = @test_module.decode(schema, small)
    end

    test "long" do
      {:ok, schema} = AvroExV0.parse_schema(~S("long"))
      {:ok, zero} = AvroExV0.encode(schema, 0)
      {:ok, neg_ten} = AvroExV0.encode(schema, -10)
      {:ok, ten} = AvroExV0.encode(schema, 10)
      {:ok, big} = AvroExV0.encode(schema, 2_147_483_647)
      {:ok, small} = AvroExV0.encode(schema, -2_147_483_647)

      assert {:ok, 0} = @test_module.decode(schema, zero)
      assert {:ok, -10} = @test_module.decode(schema, neg_ten)
      assert {:ok, 10} = @test_module.decode(schema, ten)
      assert {:ok, 2_147_483_647} = @test_module.decode(schema, big)
      assert {:ok, -2_147_483_647} = @test_module.decode(schema, small)
    end

    test "float" do
      {:ok, schema} = AvroExV0.parse_schema(~S("float"))
      {:ok, zero} = AvroExV0.encode(schema, 0.0)
      {:ok, big} = AvroExV0.encode(schema, 256.25)

      assert {:ok, 0.0} = @test_module.decode(schema, zero)
      assert {:ok, 256.25} = @test_module.decode(schema, big)
    end

    test "double" do
      {:ok, schema} = AvroExV0.parse_schema(~S("double"))
      {:ok, zero} = AvroExV0.encode(schema, 0.0)
      {:ok, big} = AvroExV0.encode(schema, 256.25)

      assert {:ok, 0.0} = @test_module.decode(schema, zero)
      assert {:ok, 256.25} = @test_module.decode(schema, big)
    end

    test "bytes" do
      {:ok, schema} = AvroExV0.parse_schema(~S("bytes"))
      {:ok, bytes} = AvroExV0.encode(schema, <<222, 213, 194, 34, 58, 92, 95, 62>>)

      assert {:ok, <<222, 213, 194, 34, 58, 92, 95, 62>>} = @test_module.decode(schema, bytes)
    end

    test "string" do
      {:ok, schema} = AvroExV0.parse_schema(~S("string"))
      {:ok, bytes} = AvroExV0.encode(schema, "Hello there 🕶")

      assert {:ok, "Hello there 🕶"} = @test_module.decode(schema, bytes)
    end
  end

  describe "complex types" do
    test "record" do
      {:ok, schema} = AvroExV0.parse_schema(~S({"type": "record", "name": "MyRecord", "fields": [
        {"type": "int", "name": "a"},
        {"type": "int", "name": "b", "aliases": ["c", "d"]},
        {"type": "string", "name": "e"}
      ]}))

      {:ok, encoded_message} = AvroExV0.encode(schema, %{"a" => 1, "b" => 2, "e" => "Hello world!"})

      assert {:ok, %{"a" => 1, "b" => 2, "e" => "Hello world!"}} =
               @test_module.decode(schema, encoded_message)
    end

    test "union" do
      {:ok, schema} = AvroExV0.parse_schema(~S(["null", "int"]))

      {:ok, encoded_null} = AvroExV0.encode(schema, nil)
      {:ok, encoded_int} = AvroExV0.encode(schema, 25)

      assert {:ok, nil} = @test_module.decode(schema, encoded_null)
      assert {:ok, 25} = @test_module.decode(schema, encoded_int)
    end

    test "array" do
      {:ok, schema} = AvroExV0.parse_schema(~S({"type": "array", "items": ["null", "int"]}))

      {:ok, encoded_array} = AvroExV0.encode(schema, [1, 2, 3, nil, 4, 5, nil])

      assert {:ok, [1, 2, 3, nil, 4, 5, nil]} = @test_module.decode(schema, encoded_array)
    end

    test "empty array" do
      {:ok, schema} = AvroExV0.parse_schema(~S({"type": "array", "items": ["null", "int"]}))

      {:ok, encoded_array} = AvroExV0.encode(schema, [])

      assert {:ok, []} = @test_module.decode(schema, encoded_array)
    end

    test "map" do
      {:ok, schema} = AvroExV0.parse_schema(~S({"type": "map", "values": ["null", "int"]}))

      {:ok, encoded_array} = AvroExV0.encode(schema, %{"a" => 1, "b" => nil, "c" => 3})

      assert {:ok, %{"a" => 1, "b" => nil, "c" => 3}} = @test_module.decode(schema, encoded_array)
    end

    test "empty map" do
      {:ok, schema} = AvroExV0.parse_schema(~S({"type": "map", "values": ["null", "int"]}))

      {:ok, encoded_map} = AvroExV0.encode(schema, %{})

      assert {:ok, %{}} = @test_module.decode(schema, encoded_map)
    end

    test "enum" do
      {:ok, schema} =
        AvroExV0.parse_schema(
          ~S({"type": "enum", "name": "Suit", "symbols": ["heart", "spade", "diamond", "club"]})
        )

      {:ok, club} = AvroExV0.encode(schema, "club")
      {:ok, heart} = AvroExV0.encode(schema, "heart")
      {:ok, diamond} = AvroExV0.encode(schema, "diamond")
      {:ok, spade} = AvroExV0.encode(schema, "spade")

      assert {:ok, "club"} = @test_module.decode(schema, club)
      assert {:ok, "heart"} = @test_module.decode(schema, heart)
      assert {:ok, "diamond"} = @test_module.decode(schema, diamond)
      assert {:ok, "spade"} = @test_module.decode(schema, spade)
    end

    test "fixed" do
      {:ok, schema} = AvroExV0.parse_schema(~S({"type": "fixed", "name": "SHA", "size": "40"}))
      sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      {:ok, encoded_sha} = AvroExV0.encode(schema, sha)
      assert {:ok, ^sha} = @test_module.decode(schema, encoded_sha)
    end
  end

  describe "decode logical types" do
    test "datetime micros" do
      now = DateTime.utc_now()

      {:ok, micro_schema} =
        AvroExV0.parse_schema(~S({"type": "long", "logicalType":"timestamp-micros"}))

      {:ok, micro_encode} = AvroExV0.encode(micro_schema, now)
      assert {:ok, ^now} = @test_module.decode(micro_schema, micro_encode)
    end

    test "datetime millis" do
      now = DateTime.utc_now() |> DateTime.truncate(:millisecond)

      {:ok, milli_schema} =
        AvroExV0.parse_schema(~S({"type": "long", "logicalType":"timestamp-millis"}))

      {:ok, milli_encode} = AvroExV0.encode(milli_schema, now)
      assert {:ok, ^now} = @test_module.decode(milli_schema, milli_encode)
    end

    test "datetime nanos" do
      now = DateTime.utc_now()

      {:ok, nano_schema} =
        AvroExV0.parse_schema(~S({"type": "long", "logicalType":"timestamp-nanos"}))

      {:ok, nano_encode} = AvroExV0.encode(nano_schema, now)
      assert {:ok, ^now} = @test_module.decode(nano_schema, nano_encode)
    end

    test "time micros" do
      now = Time.utc_now() |> Time.truncate(:microsecond)

      {:ok, micro_schema} = AvroExV0.parse_schema(~S({"type": "long", "logicalType":"time-micros"}))
      {:ok, micro_encode} = AvroExV0.encode(micro_schema, now)
      assert {:ok, ^now} = @test_module.decode(micro_schema, micro_encode)
    end

    test "time millis" do
      now = Time.utc_now() |> Time.truncate(:millisecond)

      {:ok, milli_schema} = AvroExV0.parse_schema(~S({"type": "int", "logicalType":"time-millis"}))
      {:ok, milli_encode} = AvroExV0.encode(milli_schema, now)
      {:ok, time} = @test_module.decode(milli_schema, milli_encode)

      assert Time.truncate(time, :millisecond) == now
    end
  end
end
