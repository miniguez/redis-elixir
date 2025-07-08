defmodule Server.RDB do
  import Bitwise
  @moduledoc """
  Module for parsing Redis RDB files.
  """

  # String type encoding in RDB
  @string_type 0

  # RDB opcodes
  @opcode_aux 0xFA
  @opcode_resizedb 0xFB
  @opcode_expiretime_ms 0xFC
  @opcode_expiretime_s 0xFD
  @opcode_selectdb 0xFE
  @opcode_eof 0xFF

  def load_file(rdb_path) do
    IO.puts("Server.RDB: Attempting to read RDB file at #{rdb_path}")
    case File.read(rdb_path) do
      {:ok, binary_content} ->
        IO.puts("Server.RDB: Successfully read #{byte_size(binary_content)} bytes from RDB file.")
        parse_rdb(binary_content)
      {:error, reason} ->
        IO.puts("Server.RDB: Error reading RDB file #{rdb_path}: #{reason}")
        :error # Or return a more specific error tuple
    end
  end

  defp parse_rdb(<< "REDIS", _version::binary-size(4), rest::binary >>) do
    IO.puts("Server.RDB: RDB magic string and version matched.")
    # Skip over any auxiliary fields (0xFA) and database selector (0xFE) / resize DB (0xFB)
    # We are looking for the first key-value pair.
    process_rdb_sections(rest)
  end
  defp parse_rdb(other_binary) do
    IO.puts("Server.RDB: Invalid RDB format - magic string 'REDIS' not found. Got: #{inspect :binary.part(other_binary, 0, 5)}")
    :error
  end

  # Process sections until we find a key-value pair or EOF
  defp process_rdb_sections(<< @opcode_eof, _rest::binary >>) do
    IO.puts("Server.RDB: Reached EOF before finding a key-value pair.")
    :ok # No keys found or finished parsing
  end

  defp process_rdb_sections(<< @opcode_selectdb, rest_after_opcode::binary >>) do
    IO.puts("Server.RDB: Skipping SELECTDB (0xFE) opcode.")
    case parse_rdb_length_prefixed_integer(rest_after_opcode) do
      nil ->
        IO.puts("Server.RDB: Not enough data for SELECTDB db_index.")
        :error
      {:error, reason} ->
        IO.puts("Server.RDB: Error parsing SELECTDB db_index: #{inspect(reason)}")
        :error
      # Success case: {_db_index, rest_after_db_index}
      # We don't actually use _db_index for this problem, just skip it.
      {_db_index_val, rest_after_db_val} ->
        process_rdb_sections(rest_after_db_val)
    end
  end

  defp process_rdb_sections(<< @opcode_resizedb, rest::binary >>) do
    IO.puts("Server.RDB: Skipping RESIZEDB (0xFB) opcode.")
    # RESIZEDB has two length-prefixed integers: db_size and expires_size
    with {:ok, _db_size, rest_after_db_size} <- parse_length_and_skip(rest),
         {:ok, _expires_size, rest_after_expires_size} <- parse_length_and_skip(rest_after_db_size)
    do
      process_rdb_sections(rest_after_expires_size)
    else
      _error ->
        IO.puts("Server.RDB: Error parsing lengths for RESIZEDB.")
        :error
    end
  end

defp process_rdb_sections(<< @opcode_aux, rest::binary >>) do
  IO.puts("Server.RDB: Processing AUX field (0xFA).")
  case parse_rdb_string(rest) do # For AUX key
    {:ok, _aux_key, rest_after_key} ->
      # IO.puts("Server.RDB: Parsed AUX key: #{inspect(_aux_key)}")
      if byte_size(rest_after_key) > 0 do # Check if there's data for a value
        case parse_rdb_length(rest_after_key) do # Determine value type/length
          {length, value_content_rest} when is_integer(length) -> # Standard string AUX value
            if byte_size(value_content_rest) >= length do
              <<_val_str::binary-size(length), rest_after_val_skip::binary>> = value_content_rest
              # IO.puts("Server.RDB: Skipped AUX value string of length #{length}.")
              process_rdb_sections(rest_after_val_skip)
            else
              IO.puts("Server.RDB: Error parsing AUX field value - not enough data for string of length #{length}.")
              :error
            end

          {:special_encoding, subtype, binary_after_indicator_byte} -> # Specially encoded AUX value (e.g., integer)
            # IO.puts("Server.RDB: AUX value has special encoding subtype #{subtype}.")
            case skip_special_encoded_aux_value(subtype, binary_after_indicator_byte) do
              {:ok, rest_after_val_skip} ->
                process_rdb_sections(rest_after_val_skip)
              {:error, reason_skip} ->
                IO.puts("Server.RDB: Error skipping special AUX value: #{inspect(reason_skip)}.")
                :error
            end

          nil -> # Not enough data to determine length type for AUX value
            IO.puts("Server.RDB: Not enough data for AUX value length determination (got nil from parse_rdb_length).")
            :error
        end # End of case parse_rdb_length
      else # No data for AUX value after key
        IO.puts("Server.RDB: Error parsing AUX field - no data for value after key.")
        :error
      end # End of if byte_size(rest_after_key) > 0
    {:error, reason_key_parse} -> # Error parsing AUX key string
      IO.puts("Server.RDB: Error parsing AUX field key: #{inspect(reason_key_parse)}")
      :error
    nil -> # Not enough data for AUX key string
      IO.puts("Server.RDB: Error parsing AUX field - not enough data for key.")
      :error
  end # End of case parse_rdb_string (AUX key)
end

  # Skip expiry opcodes if present before a key type
  defp process_rdb_sections(<< @opcode_expiretime_s, _timestamp::little-unsigned-integer-size(32), type_byte, rest::binary >>) do
    IO.puts("Server.RDB: Skipping EXPIRETIME_S (0xFD) opcode.")
    parse_key_value_pair(type_byte, rest)
  end

  defp process_rdb_sections(<< @opcode_expiretime_ms, _timestamp::little-unsigned-integer-size(64), type_byte, rest::binary >>) do
    IO.puts("Server.RDB: Skipping EXPIRETIME_MS (0xFC) opcode.")
    parse_key_value_pair(type_byte, rest)
  end

  # Actual key-value pair (or other opcodes not handled above)
  defp process_rdb_sections(<< type_byte, rest::binary >>) do
    # This is assumed to be the start of a key-value pair or an unhandled opcode.
    # For this task, we expect it to be the single string key-value pair.
    case parse_key_value_pair(type_byte, rest) do
      {:ok_kv_stored_final} ->
        IO.puts("Server.RDB: Successfully parsed and stored the single key-value pair. RDB loading complete.")
        :ok # This :ok will propagate up, terminating RDB processing.
      {:error_wrong_type, actual_type} ->
        IO.puts("Server.RDB: Expected string key-value type (0) but found type #{inspect(actual_type)}. Stopping parse.")
        :error
      {:error_parsing_kv, reason} ->
        IO.puts("Server.RDB: Error while parsing key-value content: #{inspect(reason)}. Stopping parse.")
        :error
    end
  end

  defp process_rdb_sections(<<>>) do
    IO.puts("Server.RDB: Reached end of binary unexpectedly while processing sections.")
    :ok # Or :error if this is not expected
  end


  defp parse_key_value_pair(type_byte, rest_after_type) do
    # Problem statement: "The RDB contains a single clave."
    # "un byte de tipo seguido de un byte (o más) que indica el largo de la cadena"
    # This implies value_type, then key_string, then value_string.
    # For this task, we only care about string types.
    if type_byte == @string_type do
      IO.puts("Server.RDB: Found string type (0x00). Parsing key...")
      case parse_rdb_string(rest_after_type) do
        {:ok, key_string, rest_after_key} ->
          IO.puts("Server.RDB: Parsed key: #{inspect(key_string)}. Parsing value...")
          case parse_rdb_string(rest_after_key) do
            {:ok, value_string, _rest_after_value} ->
              IO.puts("Server.RDB: Parsed value: #{inspect(value_string)} for key: #{inspect(key_string)}")
              Server.Store.set(key_string, value_string)
              IO.puts("Server.RDB: Stored key '#{key_string}' with value '#{value_string}' into Server.Store.")
              {:ok_kv_stored_final} # Signal that the one KV is done.
            {:error, reason_val} ->
              IO.puts("Server.RDB: Error parsing value string: #{reason_val}")
              {:error_parsing_kv, {:value_string_parse_error, reason_val}}
            nil ->
              IO.puts("Server.RDB: Not enough data for value string.")
              {:error_parsing_kv, :not_enough_data_for_value_string}
          end
        {:error, reason_key} ->
          IO.puts("Server.RDB: Error parsing key string: #{reason_key}")
          {:error_parsing_kv, {:key_string_parse_error, reason_key}}
        nil ->
          IO.puts("Server.RDB: Not enough data for key string.")
          {:error_parsing_kv, :not_enough_data_for_key_string}
      end
    else
      # This `type_byte` is not @string_type, so it's not the expected KV pair.
      {:error_wrong_type, type_byte}
    end
  end

  # Helper to parse RDB length encoding.
  # Returns {length, rest_of_binary} or {:error, reason} or nil if not enough data.
  defp parse_rdb_length(<<byte1, rest::binary>>) do
    # First two bits of byte1 determine encoding type
    # <<fb::2, sb::6>> = <<byte1>> # fb = first_bits, sb = six_bits
    first_bits = byte1 >>> 6 # Right shift to get the first 2 bits

    cond do
      # 00xxxxxx: Length is in the lower 6 bits of this byte
      first_bits == 0b00 ->
        length = byte1 &&& 0x3F # Mask to get lower 6 bits
        {length, rest}

      # 01xxxxxx: Length is in lower 6 bits of this byte + next byte
      first_bits == 0b01 ->
        if byte_size(rest) >= 1 do
          <<byte2, rest_after_byte2::binary>> = rest
          length = ((byte1 &&& 0x3F) <<< 8) + byte2 # (6 bits from byte1 shifted left) + byte2
          {length, rest_after_byte2}
        else
          nil # Not enough data
        end

      # 10xxxxxx: Next 4 bytes are the length (discard lower 6 bits of this byte)
      first_bits == 0b10 ->
        if byte_size(rest) >= 4 do
          <<len::big-unsigned-integer-size(32), rest_after_len::binary>> = rest
          {len, rest_after_len}
        else
          nil # Not enough data
        end

      # 11xxxxxx: Special encoded string (integer, LZF compressed).
      first_bits == 0b11 ->
        encoding_subtype = byte1 &&& 0x3F # Lower 6 bits define the special type
        {:special_encoding, encoding_subtype, rest}
    end
  end
  defp parse_rdb_length(<<>>) do
    nil # Not enough data
  end

  # Parses a length-prefixed string from binary data.
  # Returns {:ok, string, rest_of_binary} or {:error, reason} or nil if not enough data
  defp parse_rdb_string(binary_data) do
    case parse_rdb_length(binary_data) do
      {length, rest_after_length} when is_integer(length) and length >= 0 ->
        if byte_size(rest_after_length) >= length do
          <<string_content::binary-size(length), rest_after_string::binary>> = rest_after_length
          {:ok, string_content, rest_after_string}
        else
          IO.puts("Server.RDB.parse_rdb_string: Not enough data for string of length #{length}. Have #{byte_size(rest_after_length)} bytes.")
          nil # Not enough data for the string itself
        end
      {:special_encoding, subtype, _rest_after_indicator} ->
        IO.puts("Server.RDB.parse_rdb_string: Encountered special encoding (subtype #{subtype}) when expecting a string length.")
        {:error, {:unexpected_special_encoding_for_string, subtype}}
      # Removed unreachable {:error, reason} clause, as parse_rdb_length doesn't return it.
      nil ->
        IO.puts("Server.RDB.parse_rdb_string: Not enough data for length parsing (got nil from parse_rdb_length).")
        nil # Propagate nil, or return a specific error like {:error, :not_enough_data_for_length}
    end
  end

  # Helper to parse length for SELECTDB/RESIZEDB and skip it
  # This is a simplified version for skipping, assumes length is small and fits in 1-2 bytes.
  defp parse_length_and_skip(<<byte1, rest::binary>>) do
    first_bits = byte1 >>> 6
    cond do
      first_bits == 0b00 -> # Length in 6 bits of current byte
        {:ok, byte1 &&& 0x3F, rest}
      first_bits == 0b01 -> # Length in 6 bits + next byte
        if byte_size(rest) >= 1 do
          <<byte2, rest_after_byte2::binary>> = rest
          {:ok, ((byte1 &&& 0x3F) <<< 8) + byte2, rest_after_byte2}
        else
          {:error, "not enough data for 2-byte length"}
        end
      first_bits == 0b10 -> # Length in next 4 bytes
         if byte_size(rest) >= 4 do
          <<len_val::big-unsigned-integer-size(32), rest_after_len::binary>> = rest
          {:ok, len_val, rest_after_len}
        else
          {:error, "not enough data for 5-byte length encoding"}
        end
      true -> # Special encoding, not handled for simple skip
        {:error, "unsupported length encoding for skip"}
    end
  end
  defp parse_length_and_skip(<<>>) do
    {:error, "not enough data for length parsing"}
  end

  # Helper to parse length-prefixed integer (used by SELECTDB, RESIZEDB)
  # Returns {value, rest_binary} or {:error, reason} or nil
  defp parse_rdb_length_prefixed_integer(<<byte1, rest::binary>>) do
    first_bits = byte1 >>> 6
    cond do
      # 00xxxxxx -> integer is in the lower 6 bits
      first_bits == 0b00 -> {byte1 &&& 0x3F, rest}
      # 01xxxxxx -> integer is in (lower 6 bits of byte1 << 8) + byte2
      first_bits == 0b01 ->
        if byte_size(rest) >= 1 do
          <<byte2, rest_after_byte2::binary>> = rest
          {((byte1 &&& 0x3F) <<< 8) + byte2, rest_after_byte2}
        else
          nil # Not enough data
        end
      # 10xxxxxx -> integer is in the next 4 bytes (byte1's lower 6 bits ignored)
      # This case is typically for string lengths, not simple integers like DB index.
      # RDB spec says "length-encoded integer", usually means the 00 or 01 forms for small numbers.
      # For simplicity, we might not hit this for db_number.
      # If it's truly a "length-encoded integer" it might also be one of the special 11xxxxxx types if it means "integer object"
      # but for db_number it's simpler.
      # Let's assume for db_number, it will be small.
      true -> {:error, "Unsupported length encoding for db_number or simple integer: #{byte1}"}
    end
  end
  defp parse_rdb_length_prefixed_integer(<<>>) do
    nil # Not enough data
  end

  # Removed @doc for private function
  defp skip_special_encoded_aux_value(subtype, binary_data) do
    # subtype 0 (0b00): integer encoded as 8-bit signed int (next 1 byte)
    # subtype 1 (0b01): integer encoded as 16-bit signed int (next 2 bytes)
    # subtype 2 (0b10): integer encoded as 32-bit signed int (next 4 bytes)
    # subtype 3 (0b11): LZF compressed string (more complex, not handled for skip yet)
    cond do
      subtype == 0b00 -> # e.g., C0 prefix from RDB dump
        if byte_size(binary_data) >= 1 do
          <<_val::binary-size(1), rest::binary>> = binary_data
          IO.puts("Server.RDB.skip_special_encoded_aux_value: Skipped 1 byte for integer encoding type #{subtype}.")
          {:ok, rest}
        else
          {:error, "not enough data for 1-byte special integer"}
        end
      subtype == 0b01 ->
        if byte_size(binary_data) >= 2 do
          <<_val::binary-size(2), rest::binary>> = binary_data
          IO.puts("Server.RDB.skip_special_encoded_aux_value: Skipped 2 bytes for integer encoding type #{subtype}.")
          {:ok, rest}
        else
          {:error, "not enough data for 2-byte special integer"}
        end
      subtype == 0b10 ->
        if byte_size(binary_data) >= 4 do
          <<_val::binary-size(4), rest::binary>> = binary_data
          IO.puts("Server.RDB.skip_special_encoded_aux_value: Skipped 4 bytes for integer encoding type #{subtype}.")
          {:ok, rest}
        else
          {:error, "not enough data for 4-byte special integer"}
        end
      subtype == 0b11 -> # LZF compressed string
        IO.puts("Server.RDB.skip_special_encoded_aux_value: LZF compressed string (subtype 3) in AUX not supported for skipping.")
        {:error, :unsupported_lzf_aux_value}
      true ->
        IO.puts("Server.RDB.skip_special_encoded_aux_value: Unknown special encoding subtype: #{subtype}")
        {:error, :unknown_special_encoding_subtype}
    end
  end
end

# Ensure the Server module knows where to find Server.RDB
# This is not strictly necessary if lib/ is in compile_paths,
# but good for clarity or if files are moved.
# No, this is not how Elixir works. The compiler will find it.
# Just need to make sure the call in Server.start is correct.
# We also need to remove the Code.ensure_loaded scaffolding there.
