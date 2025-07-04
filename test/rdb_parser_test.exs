defmodule Server.RDBParserTest do
  use ExUnit.Case, async: true
  alias Server.RDBParser

  describe "parse_length_encoded_int/1" do
    test "parses 6-bit length" do
      # Length 10 (00001010)
      binary = <<0b00001010, "rest"::binary>>
      assert {:ok, 10, "rest", nil} == RDBParser.parse_length_encoded_int(binary)
    end

    test "parses 14-bit length" do
      # Length 2000 (01000111, 11010000) -> 0x47, 0xD0
      # 0b01000111 = 71. (71 &&& 0x3F) <<< 8 = (7 &&& 0x3F) <<< 8 = 7 <<< 8 = 1792
      # 0b11010000 = 208
      # 1792 + 208 = 2000
      binary = <<0b01000111, 0b11010000, "rest"::binary>>
      assert {:ok, 2000, "rest", nil} == RDBParser.parse_length_encoded_int(binary)
    end

    test "parses 32-bit length" do
      # Length 70000 (10xxxxxx followed by 4 bytes: 00 01 11 70)
      # 0x00011170 = 65536 + 4096 + 256 + 112 = 70000
      binary = <<0b10000000, 0x00, 0x01, 0x11, 0x70, "rest"::binary>>
      assert {:ok, 70000, "rest", nil} == RDBParser.parse_length_encoded_int(binary)
    end

    test "parses special format for 8-bit integer" do
      binary = <<0b11000000, 0x05, "rest"::binary>> # 0xC0 means 8-bit int
      assert {:ok, 0, <<0x05, "rest"::binary>>, {:integer_8bit}} == RDBParser.parse_length_encoded_int(binary)
    end
     # Add more tests for other special formats (16-bit, 32-bit int, compressed) if time permits
  end

  describe "parse_string_encoded/1" do
    test "parses a simple length-prefixed string" do
      # String "hello" (length 5)
      binary = <<5, "hello"::binary, "rest"::binary>>
      assert {:ok, "hello", "rest"} == RDBParser.parse_string_encoded(binary)
    end

    test "parses an 8-bit integer as string" do
      # Integer 123 (0x7B) encoded as string
      binary = <<0b11000000, 123, "rest"::binary>>
      assert {:ok, "123", "rest"} == RDBParser.parse_string_encoded(binary)
    end

    test "parses a 16-bit integer as string" do
      # Integer 12345 (0x3039) encoded as string
      binary = <<0b11000001, 0x30, 0x39, "rest"::binary>> # 0xC1 indicates 16-bit int
      assert {:ok, "12345", "rest"} == RDBParser.parse_string_encoded(binary)
    end

    test "returns error for unsupported compressed string" do
      binary = <<0b11000011, "rest"::binary>> # 0xC3 indicates compressed string
      assert {:error, "LZF compressed strings are not supported yet"} == RDBParser.parse_string_encoded(binary)
    end
  end

  describe "parse_header/1 (internal function, testing via parse/1)" do
    test "parses a valid header" do
      # "REDIS0009"
      binary = <<"REDIS0009rest_of_rdb"::binary>>
      # We expect parse_opcodes to be called, which will hit EOF with "rest_of_rdb"
      # and for now, it will try to parse it as an opcode, leading to an error or specific handling.
      # Let's make a minimal RDB with just EOF after header for this test.
      valid_rdb_minimal = <<"REDIS0009", RDBParser.opcode_eof() :: size(8), "checksum"::binary-size(8)>>
      assert {:ok, [], %{}} == RDBParser.parse(valid_rdb_minimal)
    end

    test "returns error for invalid magic string" do
      binary = <<"INVALID0003"::binary>>
      assert {:error, "Invalid RDB magic string"} == RDBParser.parse(binary)
    end

    test "returns error for invalid RDB version format" do
      binary = <<"REDISवर्जन"::binary>> # Non-ASCII version
      assert {:error, "Invalid RDB version format: वर्जन"} == RDBParser.parse(binary)
    end
  end

  describe "parse/1 (main RDB parsing)" do
    test "parses an empty RDB (only header and EOF)" do
      eof = <<RDBParser.opcode_eof() :: size(8)>>
      checksum = <<0,0,0,0,0,0,0,0>> # 8 byte checksum
      binary = <<"REDIS0003"::binary, eof::binary, checksum::binary>>
      assert {:ok, [], %{}} == RDBParser.parse(binary)
    end

    test "parses RDB with a single key-value (string)" do
      # RDB Structure:
      # Header: "REDIS0003"
      # DB selector: FE 00 (select DB 0) - not strictly needed if only one db section
      # Value Type: 0 (string)
      # Key: "mykey" (string encoded: length 5, "mykey")
      # Value: "myvalue" (string encoded: length 7, "myvalue")
      # EOF: FF
      # Checksum: 8 bytes

      key = "mykey"
      value = "myvalue" # Value content doesn't matter for KEYS, but parser needs to skip it

      # String encode key "mykey" -> <<5, "mykey">>
      encoded_key = <<String.length(key) :: size(8), key::binary>>
      # String encode value "myvalue" -> <<7, "myvalue">>
      encoded_value = <<String.length(value) :: size(8), value::binary>>

      # Opcode for string value type
      value_type_string = <<RDBParser.value_type_string() :: size(8)>>

      eof_marker = <<RDBParser.opcode_eof() :: size(8)>>
      checksum = <<1,2,3,4,5,6,7,8>> # Dummy checksum

      # Minimal RDB: Header + ValueType + EncodedKey + EncodedValue + EOF + Checksum
      # No DB selector, AUX, RESIZEDB for simplicity here. Assumes default DB 0.
      rdb_binary = <<"REDIS0003"::binary,
                      value_type_string::binary,
                      encoded_key::binary,
                      encoded_value::binary,
                      eof_marker::binary,
                      checksum::binary>>

      IO.inspect(rdb_binary, label: "Test RDB Binary")

      case RDBParser.parse(rdb_binary) do
        {:ok, keys_list, _aux_data} ->
          assert [{^key, nil, 0}] = keys_list # {key_str, expiry_ms, db_num}
        other ->
          flunk("Parsing failed or returned unexpected structure: #{inspect(other)}")
      end
    end

    test "parses RDB with an expired key (seconds)" do
      # Header + FD (expire_s) + expiry_time (4 bytes) + ValueType + Key + Value + EOF + Checksum
      key = "expiredkey"
      encoded_key = <<String.length(key)::size(8), key::binary>>
      encoded_value = <<String.length("value")::size(8), "value"::binary>>
      value_type_string = <<RDBParser.value_type_string() :: size(8)>>

      # Expiry time in seconds (Unix timestamp) - set to past
      # DateTime.utc_now() |> DateTime.add(-3600, :second) |> DateTime.to_unix()
      # For stable test, use a fixed past timestamp, e.g., Jan 1, 2000
      past_expiry_s = DateTime.to_unix(~U[2000-01-01 00:00:00Z]) # 946684800

      opcode_expire_s = <<RDBParser.opcode_expiretime_s() :: size(8)>>

      rdb_binary = <<"REDIS0003"::binary,
                      opcode_expire_s::binary,
                      past_expiry_s::unsigned-little-integer-size(32), #Expiry Time in seconds
                      value_type_string::binary,
                      encoded_key::binary,
                      encoded_value::binary,
                      <<RDBParser.opcode_eof() :: size(8)>>,
                      <<0,0,0,0,0,0,0,0>> >>

      assert {:ok, [], _aux} = RDBParser.parse(rdb_binary) # Expired key should not be in the list
    end

    # TODO: Test for key with expiry in MS
    # TODO: Test for key that is not expired
    # TODO: Test with AUX fields
    # TODO: Test with DB selector
  end

  # Helper to access module attributes if needed, e.g. opcodes for constructing test binaries
  defp opcode_eof(), do: unquote(RDBParser.opcode_eof())
  defp value_type_string(), do: unquote(RDBParser.value_type_string())
  defp opcode_expiretime_s(), do: unquote(RDBParser.opcode_expiretime_s())

end
