defmodule AvroExV0.Schema.Context do
  alias AvroExV0.Schema
  alias AvroExV0.Schema.{Array, Fixed, Primitive, Record, Union}
  alias AvroExV0.Schema.Enum, as: AvroEnum
  alias AvroExV0.Schema.Record.Field

  defstruct names: %{}

  @type t :: %__MODULE__{
          names: %{Schema.full_name() => Record.t()}
        }

  def add_schema(%__MODULE__{} = context, %Primitive{}), do: context

  def add_schema(%__MODULE__{} = context, %AvroExV0.Schema.Map{values: values}),
    do: add_schema(context, values)

  def add_schema(%__MODULE__{} = context, %Array{items: items}), do: add_schema(context, items)

  def add_schema(%__MODULE__{} = context, %Union{possibilities: possibilities}) do
    Enum.reduce(possibilities, context, fn schema, %__MODULE__{} = context ->
      add_schema(context, schema)
    end)
  end

  def add_schema(%__MODULE__{} = context, %Fixed{} = schema) do
    Enum.reduce(schema.qualified_names, context, fn name, %__MODULE__{} = context ->
      add_name(context, name, schema)
    end)
  end

  def add_schema(%__MODULE__{} = context, %Record{} = schema) do
    context =
      Enum.reduce(schema.qualified_names, context, fn name, %__MODULE__{} = context ->
        add_name(context, name, schema)
      end)

    Enum.reduce(schema.fields, context, fn %Field{type: type}, %__MODULE__{} = context ->
      add_schema(context, type)
    end)
  end

  def add_schema(%__MODULE__{} = context, %AvroEnum{} = schema) do
    Enum.reduce(schema.qualified_names, context, fn name, %__MODULE__{} = context ->
      add_name(context, name, schema)
    end)
  end

  def add_schema(%__MODULE__{} = context, name) when is_binary(name) do
    context
  end

  def add_name(%__MODULE__{} = context, name, value) when is_binary(name) do
    %__MODULE__{names: Map.put_new(context.names, name, value)}
  end

  @spec lookup(t, String.t()) :: nil | Schema.schema_types()
  def lookup(%__MODULE__{} = context, name) do
    context.names[name]
  end
end
