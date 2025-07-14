defmodule Server.RDBParser do
  @moduledoc """
  Parses Redis RDB files.
  """

  require Logger
  import Bitwise

  @magic_string "REDIS"
  @rdb_version_length 4

  # RDB Opcodes
  @opcode_aux 0xFA
  @opcode_resizedb 0xFB
  @opcode_expiretime_ms 0xFC
  @opcode_expiretime_s 0xFD
  @opcode_selectdb 0xFE
  @opcode_eof 0xFF

  # Value types (only string is handled for skipping initially)
  @value_type_string 0
  # Other value types would be defined here, e.g.
  # @value_type_list 1
  # @value_type_set 2
  # ...

  # Expose opcodes for testing if needed
  def opcode_aux, do: @opcode_aux
  def opcode_resizedb, do: @opcode_resizedb
  def opcode_expiretime_ms, do: @opcode_expiretime_ms
  def opcode_expiretime_s, do: @opcode_expiretime_s
  def opcode_selectdb, do: @opcode_selectdb
  def opcode_eof, do: @opcode_eof
  def value_type_string, do: @value_type_string


  def parse(rdb_content_binary) do
    Logger.info("Starting RDB parsing...")
    # Initialize with an empty list of keys and an empty map for auxiliary data
    initial_acc = %{keys: [], aux_data: %{}, current_db: 0}

    case parse_header(rdb_content_binary) do
      {:ok, rdb_version, rest_after_header} ->
        Logger.info("RDB Version: #{rdb_version}")
        parse_opcodes(rest_after_header, initial_acc)

      {:error, reason} ->
        Logger.error("Failed to parse RDB header: #{reason}")
        {:error, reason}
    end
  end

  defp parse_opcodes(<<@opcode_eof, _::binary>>, acc) do # Ignore checksum for now
    Logger.info("Reached EOF of RDB file.")
    {:ok, Enum.reverse(acc.keys), acc.aux_data} # Return collected keys and aux_data
  end

  defp parse_opcodes(<<@opcode_aux, rest::binary>>, acc) do
    with {:ok, key_str, rest_after_key} <- parse_string_encoded(rest),
         {:ok, val_str, rest_after_val} <- parse_string_encoded(rest_after_key) do
      Logger.debug("Parsed AUX field: #{key_str} = #{val_str}")
      parse_opcodes(rest_after_val, %{acc | aux_data: Map.put(acc.aux_data, key_str, val_str)})
    else
      {:error, reason} -> {:error, "Failed to parse AUX field: #{reason}"}
    end
  end

  defp parse_opcodes(<<@opcode_selectdb, rest::binary>>, acc) do
    with {:ok, db_number, rest_after_db_num, nil} <- parse_length_encoded_int(rest) do
      Logger.debug("Selected DB: #{db_number}")
      parse_opcodes(rest_after_db_num, %{acc | current_db: db_number})
    else
      {:error, reason} -> {:error, "Failed to parse SELECTDB: #{reason}"}
      _ -> {:error, "Failed to parse SELECTDB: unexpected return from parse_length_encoded_int"}
    end
  end

  defp parse_opcodes(<<@opcode_resizedb, rest::binary>>, acc) do
    with {:ok, _db_hash_table_size, rest_after_size1, nil} <- parse_length_encoded_int(rest),
         {:ok, _expire_hash_table_size, rest_after_size2, nil} <- parse_length_encoded_int(rest_after_size1) do
      Logger.debug("Parsed RESIZEDB (sizes skipped)")
      parse_opcodes(rest_after_size2, acc)
    else
      {:error, reason} -> {:error, "Failed to parse RESIZEDB: #{reason}"}
      _ -> {:error, "Failed to parse RESIZEDB: unexpected return from parse_length_encoded_int"}
    end
  end

  # Key-value pair with expiry in milliseconds
  defp parse_opcodes(<<@opcode_expiretime_ms, expiry_ms::unsigned-little-integer-size(64), value_type::size(8), rest::binary>>, acc) do
    parse_key_value_pair(rest, value_type, expiry_ms, acc)
  end

  # Key-value pair with expiry in seconds
  defp parse_opcodes(<<@opcode_expiretime_s, expiry_s::unsigned-little-integer-size(32), value_type::size(8), rest::binary>>, acc) do
    expiry_ms = expiry_s * 1000
    parse_key_value_pair(rest, value_type, expiry_ms, acc)
  end

  # Key-value pair without expiry (value_type is the first byte)
  defp parse_opcodes(<<value_type::size(8), rest::binary>>, acc) do
    parse_key_value_pair(rest, value_type, nil, acc)
  end

  defp parse_opcodes(<<>>, acc) do # Should ideally be caught by EOF
    Logger.warning("Reached end of binary unexpectedly without EOF marker.")
    {:ok, Enum.reverse(acc.keys), acc.aux_data}
  end

  defp parse_opcodes(unhandled_binary, _acc) do
    Logger.error("Unhandled RDB data segment starting with: #{inspect :binary.first(unhandled_binary)}")
    {:error, "Unhandled RDB data segment"}
  end


  defp parse_key_value_pair(data, value_type, expiry_ms, acc) do
    System.monotonic_time(:millisecond) # Using monotonic for internal expiry check during parsing

    is_expired =
      cond do
        is_nil(expiry_ms) -> false
        expiry_ms < DateTime.to_unix(DateTime.utc_now(), :millisecond) -> true
        true -> false
      end

    with {:ok, key_str, rest_after_key} <- parse_string_encoded(data) do
      # For now, we only fully parse/skip string values. Other types will cause an error.
      case value_type do
        @value_type_string ->
          with {:ok, _value_str, rest_after_val} <- parse_string_encoded(rest_after_key) do
            if is_expired do
              Logger.debug("Key '#{key_str}' is expired. Skipping.")
              parse_opcodes(rest_after_val, acc)
            else
              Logger.debug("Parsed key: #{key_str} (DB: #{acc.current_db}, Expiry: #{expiry_ms || "N/A"})")
              new_keys = [{key_str, expiry_ms, acc.current_db} | acc.keys]
              parse_opcodes(rest_after_val, %{acc | keys: new_keys})
            end
          else
            {:error, reason} -> {:error, "Failed to parse value for key '#{key_str}': #{reason}"}
          end
        _ ->
          Logger.error("Unsupported value type: #{value_type} for key '#{key_str}'. Cannot skip value.")
          {:error, "Unsupported value type: #{value_type}"}
      end
    else
      {:error, reason} -> {:error, "Failed to parse key: #{reason}"}
    end
  end


  defp parse_header(<<@magic_string::binary, version_binary::binary-size(@rdb_version_length), rest::binary>>) do
    # The version is stored as ASCII characters, e.g., "0009"
    case Integer.parse(version_binary) do
      {version, ""} -> {:ok, version, rest}
      _ -> {:error, "Invalid RDB version format: #{version_binary}"}
    end
  end

  defp parse_header(_), do: {:error, "Invalid RDB magic string"}

  @doc """
  Parses a length-encoded integer from the binary data.
  Returns `{:ok, length, rest_of_binary, special_format_info}` or `{:error, reason}`.
  `special_format_info` is `nil` if not a special format, otherwise `{format_type}`
  where `format_type` is `:integer_8bit`, `:integer_16bit`, `:integer_32bit`, or `:compressed_string`.
  """
  def parse_length_encoded_int(<<first_byte::size(8), rest::binary>>) do
    case first_byte &&& 0xC0 do # Check the two most significant bits
      0x00 -> # 00xxxxxx: The next 6 bits represent the length
        length = first_byte &&& 0x3F
        {:ok, length, rest, nil}

      0x40 -> # 01xxxxxx: Read one additional byte. The combined 14 bits represent the length
        <<next_byte::size(8), rest_after_next_byte::binary>> = rest
        length = ((first_byte &&& 0x3F) <<< 8) ||| next_byte
        {:ok, length, rest_after_next_byte, nil}

      0x80 -> # 10xxxxxx: Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
        <<length_bytes::binary-size(4), rest_after_length::binary>> = rest
        length = :binary.decode_unsigned(length_bytes, :big) # RDB uses big-endian for this
        {:ok, length, rest_after_length, nil}

      0xC0 -> # 11xxxxxx: The next object is encoded in a special format. The remaining 6 bits indicate the format.
        format_specifier = first_byte &&& 0x3F
        case format_specifier do
          0 -> {:ok, 0, rest, {:integer_8bit}} # Length is not used, indicates 8-bit integer follows
          1 -> {:ok, 0, rest, {:integer_16bit}} # Length is not used, indicates 16-bit integer follows
          2 -> {:ok, 0, rest, {:integer_32bit}} # Length is not used, indicates 32-bit integer follows
          3 -> {:ok, 0, rest, {:compressed_string}} # Length is not used, indicates compressed string follows
          _ -> {:error, "Reserved special format specifier: #{format_specifier}"}
        end
    end
  end
  def parse_length_encoded_int(_), do: {:error, "Insufficient data for length encoding"}

  @doc """
  Parses a string-encoded value from the binary data.
  Returns `{:ok, string_value, rest_of_binary}` or `{:error, reason}`.
  """
  def parse_string_encoded(binary_data) do
    case parse_length_encoded_int(binary_data) do
      {:ok, length, rest, nil} -> # Length-prefixed string
        if byte_size(rest) >= length do
          <<string_data::binary-size(length), rest_after_string::binary>> = rest
          {:ok, string_data, rest_after_string}
        else
          {:error, "Insufficient data for string of length #{length}"}
        end

      {:ok, _length, rest, {:integer_8bit}} ->
        <<int_val::integer-size(8), rest_after_int::binary>> = rest
        {:ok, Integer.to_string(int_val), rest_after_int}

      {:ok, _length, rest, {:integer_16bit}} ->
        <<int_val::integer-signed-size(16)-big, rest_after_int::binary>> = rest # Assuming big-endian for RDB integers
        {:ok, Integer.to_string(int_val), rest_after_int}

      {:ok, _length, rest, {:integer_32bit}} ->
        <<int_val::integer-signed-size(32)-big, rest_after_int::binary>> = rest # Assuming big-endian for RDB integers
        {:ok, Integer.to_string(int_val), rest_after_int}

      {:ok, _length, _rest, {:compressed_string}} ->
        Logger.warning("LZF compressed string encountered, not supported for keys yet.")
        {:error, "LZF compressed strings are not supported yet"}

      {:error, reason} ->
        {:error, "Failed to parse length for string: #{reason}"}

    end
  end
end
