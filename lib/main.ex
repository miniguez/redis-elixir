defmodule Server.Store do
  use Agent

  @name __MODULE__

  def start_link(_opts) do
    # Initial state: %{key => {value, expiration_time_monotonic_ms}}
    # For RDB loaded keys, value might be a special marker or map like %{rdb_loaded: true, original_expiry_unix_ms: ...}
    Agent.start_link(fn -> %{} end, name: @name)
  end

  def get(key) do
    Agent.get_and_update(@name, fn state ->
      current_time_monotonic_ms = System.monotonic_time(:millisecond)
      case Map.get(state, key) do
        nil ->
          {nil, state} # Key not found
        {value_data, expiration_time_monotonic_ms} ->
          if not is_nil(expiration_time_monotonic_ms) and current_time_monotonic_ms >= expiration_time_monotonic_ms do
            {nil, Map.delete(state, key)} # Key expired
          else
            # If value_data is a map for RDB, extract actual value if needed, or return as is.
            # For now, the `KEYS` command only needs existence, not the value itself.
            # The `GET` command would need to handle this if it were to retrieve RDB values.
            actual_value = if is_map(value_data) and Map.has_key?(value_data, :rdb_loaded), do: value_data.original_value, else: value_data
            {actual_value, state}
          end
      end
    end)
  end

  def set(key, value, expiry_px_ms \\ nil) do
    expiration_time_monotonic_ms =
      if expiry_px_ms do
        System.monotonic_time(:millisecond) + expiry_px_ms
      else
        nil
      end
    # Standard SET command stores the direct value
    Agent.update(@name, &Map.put(&1, key, {value, expiration_time_monotonic_ms}))
  end

  def load_from_rdb(rdb_file_path) do
    case File.read(rdb_file_path) do
      {:ok, rdb_binary_content} ->
        case Server.RDBParser.parse(rdb_binary_content) do
          {:ok, parsed_keys_info, _aux_data} ->
            Agent.update(@name, fn current_state ->
              Enum.reduce(parsed_keys_info, current_state, fn {key_str, expiry_unix_ms, _db_num}, acc_state ->
                # For keys from RDB, we store a map indicating its origin and original expiry.
                # The actual value from RDB is not stored yet, placeholder `true` or the key itself.
                # If GET is to work with these, this structure needs to be handled in GET.
                # For now, KEYS just needs the key to exist.

                # Convert UNIX ms expiry to monotonic TTL if needed for consistency,
                # or store UNIX ms and handle in get.
                # For simplicity, let's store original UNIX ms and check against wall clock for RDB keys.
                # However, the `get` function currently uses monotonic time.
                # Let's adjust: if expiry_unix_ms is present, calculate remaining TTL from now.

                current_wall_time_ms = DateTime.to_unix(DateTime.utc_now(), :millisecond)
                value_to_store = %{rdb_loaded: true, original_expiry_unix_ms: expiry_unix_ms, original_value: "[RDB_VALUE_PLACEHOLDER]"}

                cond do
                  is_nil(expiry_unix_ms) -> # No expiry
                    Map.put(acc_state, key_str, {value_to_store, nil})
                  expiry_unix_ms > current_wall_time_ms -> # Not expired yet
                    # Calculate remaining TTL from now for monotonic clock
                    remaining_ttl_ms = expiry_unix_ms - current_wall_time_ms
                    expiration_time_monotonic_ms = System.monotonic_time(:millisecond) + remaining_ttl_ms
                    Map.put(acc_state, key_str, {value_to_store, expiration_time_monotonic_ms})
                  true -> # Already expired based on wall clock
                    IO.puts("RDB key '#{key_str}' already expired based on wall clock. Not loading.")
                    acc_state # Don't add to store
                end
              end)
            end)
            IO.puts("Successfully loaded #{length(parsed_keys_info)} keys from RDB: #{rdb_file_path}")
            :ok
          {:error, reason} ->
            IO.puts("Failed to parse RDB file #{rdb_file_path}: #{reason}")
            {:error, "RDB parsing failed: #{reason}"}
        end
      {:error, reason} ->
        IO.puts("Failed to read RDB file #{rdb_file_path}: #{reason}")
        # If the file doesn't exist, it's not an error for Redis, it just starts empty.
        # Other read errors might be more critical.
        if reason == :enoent do
          IO.puts("RDB file not found at #{rdb_file_path}. Starting with an empty state.")
          :ok # Not an error if file doesn't exist
        else
          {:error, "RDB file read error: #{reason}"}
        end
    end
  end

  def get_all_keys() do
    Agent.get_and_update(@name, fn state ->
      current_time_monotonic_ms = System.monotonic_time(:millisecond)
      valid_keys =
        state
        |> Enum.filter(fn {_key, {_value_data, expiration_time_monotonic_ms}} ->
          is_nil(expiration_time_monotonic_ms) or current_time_monotonic_ms < expiration_time_monotonic_ms
        end)
        |> Enum.map(fn {key, _value} -> key end) # Extract just the key string

      # The first element of the tuple is the return value of Agent.get_and_update
      # The second is the new state (which is unchanged in this case as we are only reading)
      {valid_keys, state}
    end)
  end

end

defmodule Server.Config do
  use Agent

  # Starts the agent with initial configuration values
  # opts should be a map like %{dir: "/path/to/dir", dbfilename: "dump.rdb"}
  def start_link(opts) do
    initial_config = %{
      "dir" => Map.get(opts, :dir),
      "dbfilename" => Map.get(opts, :dbfilename)
    }
    Agent.start_link(fn -> initial_config end, name: __MODULE__)
  end

  # Retrieves a configuration value by key
  def get(key) do
    Agent.get(__MODULE__, &Map.get(&1, key))
  end

  # Retrieves all configuration values (optional, might be useful for debugging)
  def get_all() do
    Agent.get(__MODULE__, &(&1))
  end
end

defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  # Called by Application.ensure_all_started, typically as Server.start(:normal, [])
  # due to `mod: {Server, []}` in mix.exs.
  def start(start_type, static_args_from_mix) do
    IO.puts("Server.start called with start_type: #{inspect(start_type)}, static_args_from_mix: #{inspect(static_args_from_mix)}")

    # Fetch CLI arguments from the application environment
    # Provide a default of empty map if :cli_options is not found
    cli_options_from_env = Application.get_env(:codecrafters_redis, :cli_options, %{})
    IO.inspect(cli_options_from_env, label: "Server.start: Value for :cli_options from app env")

    # Validate fetched arguments and prepare cli_options_map for Server.Config
    cli_options_map =
      if is_map(cli_options_from_env) and map_size(cli_options_from_env) > 0 do
        IO.puts("Server.start successfully fetched args from application environment: #{inspect(cli_options_from_env)}")
        cli_options_from_env
      else
        IO.puts("Warning: Server.start fetched nil or empty/invalid args from application environment. Using empty map for Server.Config.")
        # IO.inspect(cli_options_from_env, label: "Fetched from application environment was invalid") # Redundant if logged by inspect above
        %{} # Default to empty map
      end
    #IO.puts("Server.start converted CLI options to map: #{inspect(cli_options_map)}")

    children = [
      {Server.Config, cli_options_map}, # Start Config first
      Server.Store,                     # Then Store
      {Task, fn -> Server.load_rdb_and_start_listening() end} # Then load RDB and listen
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def load_rdb_and_start_listening() do
    # Load RDB data after Server.Config and Server.Store are up
    dir = Server.Config.get("dir")
    dbfilename = Server.Config.get("dbfilename")

    if dir && dbfilename do
      rdb_path = Path.join(dir, dbfilename)
      IO.puts("Attempting to load RDB file from: #{rdb_path}")
      case Server.Store.load_from_rdb(rdb_path) do
        :ok -> IO.puts("RDB loading process completed.")
        {:error, reason} -> IO.puts("Error during RDB loading: #{inspect(reason)}. Server will start with empty/current state.")
      end
    else
      IO.puts("RDB directory or filename not configured. Skipping RDB load.")
    end

    # Proceed to listen for connections
    Server.listen()
  end

  @doc """
  Listen for incoming connections
  """
  def listen(attempts_left \\ 3) do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    IO.puts("Logs from your program will appear here!")

    # Abre un socket TCP en el puerto 6379 (puerto estándar de Redis)
    case :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true]) do
      {:ok, socket} ->
        IO.puts("Server listening on port 6379")
        accept_connections(socket)
      {:error, :eaddrinuse} when attempts_left > 1 ->
        IO.puts("Port 6379 is in use, retrying in 1 second... (#{attempts_left - 1} attempts left)")
        Process.sleep(1000)
        listen(attempts_left - 1)
      {:error, reason} ->
        IO.puts("Failed to listen on port 6379 after multiple attempts: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp accept_connections(socket) do
    # Acepta conexiones entrantes en el socket
    IO.puts("Waiting to accept connection...")
    {:ok, client} = :gen_tcp.accept(socket)
    IO.puts("Connection accepted from client: #{inspect(client)}")
    # Inicia un nuevo proceso para manejar este cliente
    spawn(fn -> handle_client(client) end)
    # Continúa aceptando conexiones en el socket
    accept_connections(socket)
  end

  defp handle_client(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        IO.puts("Received data: #{inspect(data)}")
        # Aquí puedes procesar el comando recibido
        # Por ejemplo, si es un comando RESP, puedes parsearlo y responder
        # Asumiendo que el comando es RESP, lo parseamos y respondemos
        handle_command(client, data)
      #{:ok, :closed} ->
        handle_client(client)
      {:error, :closed} ->
        IO.puts("Client disconnected: #{inspect(client)}")
      {:error, reason} ->
        IO.puts("Error receiving data from client #{inspect(client)}: #{inspect(reason)}")
    end
  end

  defp handle_command(client, data) do
    parsed_command = parse_resp(data)
    IO.puts("Parsed command: #{inspect(parsed_command)}")
    case parsed_command do
      # Si el comando es PING, responde con PONG
      ["PING"] ->
        :gen_tcp.send(client, "+PONG\r\n")
        # Aquí puedes procesar el comando recibido
      ["ECHO", argument] ->
        # Respond with the argument as a RESP Bulk String
        :gen_tcp.send(client, "$#{String.length(argument)}\r\n#{argument}\r\n")
        # Si el comando es SET, almacena el valor y responde con OK
        # Handles SET key value PX expiry_ms (case-insensitive for PX)
      ["SET", key, value, option, expiry_ms_str] ->
        cond do
          String.downcase(option) == "px" ->
            case Integer.parse(expiry_ms_str) do
              {expiry_ms, ""} when expiry_ms > 0 ->
                Server.Store.set(key, value, expiry_ms)
                :gen_tcp.send(client, "+OK\r\n")
              _ ->
                :gen_tcp.send(client, "-ERR value is not an integer or out of range\r\n")
            end
          true ->
            :gen_tcp.send(client, "-ERR syntax error\r\n")
        end
      ["SET", key, value] ->
        Server.Store.set(key, value)
        :gen_tcp.send(client, "+OK\r\n")

      # Si el comando es GET, recupera el valor y responde
      ["GET", key] ->
        case Server.Store.get(key) do
          nil ->
            :gen_tcp.send(client, "$-1\r\n")
          value ->
            :gen_tcp.send(client, "$#{String.length(value)}\r\n#{value}\r\n")
        end
      # Handles CONFIG GET parameter
      ["CONFIG", "GET", param_name_input] ->
        # Parameter names in CONFIG GET are case-insensitive in Redis,
        # but we stored them as lowercase strings in Server.Config.
        # The problem asks for "dir" and "dbfilename".
        # The input param_name_input could be "dir", "DIR", "dbfilename", "DBFILENAME", etc.
        # We should fetch using the canonical lowercase name.
        param_name_to_fetch = String.downcase(param_name_input)

        case Server.Config.get(param_name_to_fetch) do
          nil ->
            # If the parameter is not found, we should respond with an error.
            IO.puts("Warning: CONFIG GET for unknown parameter '#{param_name_input}' (fetching as '#{param_name_to_fetch}')")
            :gen_tcp.send(client, "-ERR parameter '#{param_name_input}' not configured or supported\r\n")
          value when is_binary(value) ->
            # If the parameter is found, we respond with the parameter name and its value.
            response = "*2\r\n$#{String.length(param_name_input)}\r\n#{param_name_input}\r\n$#{String.length(value)}\r\n#{value}\r\n"
            :gen_tcp.send(client, response)
          _ ->
            # If the value is not a binary, we should handle it as an error.
            :gen_tcp.send(client, "-ERR internal error retrieving config for '#{param_name_input}'\r\n")
        end
      # Handles CONFIG SET parameter value
      ["KEYS", pattern_str] ->
        all_keys = Server.Store.get_all_keys() # This function needs to be added to Server.Store
        # Convert glob pattern to regex: '*' -> '.*', '?' -> '.', etc.
        # For this stage, only '*' is required by the problem description.
        # Basic glob to regex: escape special Redis chars, then convert *
        regex_pattern_str =
          pattern_str
          |> String.replace("*", ".*") # Replace glob * with regex .*
          # Add other glob conversions here if needed, e.g., ? -> .
          # Ensure the pattern matches the whole string:
          |> then(&"^#{&1}$")

        case Regex.compile(regex_pattern_str) do
          {:ok, regex} ->
            matching_keys =
              Enum.filter(all_keys, fn key ->
                Regex.match?(regex, key)
              end)

            # Format as RESP array
            response_parts = ["*#{length(matching_keys)}\r\n"]
            response_parts =
              Enum.reduce(matching_keys, response_parts, fn key, acc ->
                acc ++ ["$#{String.length(key)}\r\n#{key}\r\n"]
              end)
            :gen_tcp.send(client, IO.iodata_to_binary(response_parts))

          {:error, reason} ->
            IO.puts("Invalid regex pattern from glob '#{pattern_str}': #{reason}")
            :gen_tcp.send(client, "-ERR invalid pattern\r\n")
        end

      _ ->
        IO.puts("Unknown command raw data: #{inspect(data)}")
        IO.puts("Unknown command parsed structure: #{inspect(parsed_command)}")
        :gen_tcp.send(client, "-ERR unknown command\r\n")
    end
  end

  defp parse_resp(data) do
    case data do
      # If the data starts with a '*', it indicates an array in RESP format
      <<?*, count_char::binary-size(1), "\r\n", rest::binary>> ->
        case String.to_integer(count_char) do
          count_int when is_integer(count_int) and count_int >= 0 ->
            parse_bulk_string_array(count_int, rest, [])
          _ ->
            IO.puts("Invalid array count: #{count_char}")
            ["INVALID_RESP_COUNT"]
        end
      # If the data starts with a '$', it indicates a bulk string in RESP format
      _ ->
        # If the data does not start with '*', we assume it's a single bulk string or a simple command
        trimmed_data = String.trim(data)
        if String.length(trimmed_data) > 0 do
          String.split(trimmed_data, " ")
        else
          [""]
        end
    end
  end

  # Parses a RESP array of bulk strings
  defp parse_bulk_string_array(0, _rest, acc), do: Enum.reverse(acc)
  defp parse_bulk_string_array(_count, <<>>, acc) do
    IO.puts("Reached end of data unexpectedly while parsing bulk string array")
    Enum.reverse(acc)
  end
  defp parse_bulk_string_array(count, <<?$, rest_after_dollar::binary>>, acc) do
    # Extract the length of the bulk string
    case String.split(rest_after_dollar, "\r\n", parts: 2) do
      [length_str, rest_after_length_crlf] ->
        case String.to_integer(length_str) do
          len when is_integer(len) and len >= 0 ->
            if byte_size(rest_after_length_crlf) >= len + 2 do
              <<string_data::binary-size(len), "\r\n", rest_after_string_crlf::binary>> = rest_after_length_crlf
              parse_bulk_string_array(count - 1, rest_after_string_crlf, [string_data | acc])
            else
              IO.puts("Insufficient data for string of length #{len}")
              # If the length is not sufficient, we can return an error or handle it gracefully
              Enum.reverse(["ERROR_INSUFFICIENT_DATA" | acc])
            end
          _ ->
            IO.puts("Invalid bulk string length: #{length_str}")
            Enum.reverse(["ERROR_INVALID_LENGTH" | acc])
        end
      _ ->
        IO.puts("Malformed bulk string: missing CRLF after length")
        Enum.reverse(["ERROR_MALFORMED_BULK_STRING" | acc])
    end
  end
  defp parse_bulk_string_array(_count, other_data, acc) do
    IO.puts("Unexpected data format in parse_bulk_string_array. Expected '$', got: #{inspect other_data}")
    # If we reach here, it means the data format is unexpected
    Enum.reverse(["ERROR_UNEXPECTED_FORMAT_IN_ARRAY" | acc])
  end

end


# This CLI module might be intended to start the Server application
# For now, we assume the Server module is started as part of the :codecrafters_redis OTP app
# as defined in mix.exs (or by a top-level supervisor)

defmodule CLI do
  def main(args) do
    IO.puts("CLI arguments: #{inspect(args)}")

    parsed_args = parse_cli_args(args)
    IO.puts("Parsed CLI arguments: #{inspect(parsed_args)}")

    IO.puts("Parsed CLI arguments: #{inspect(parsed_args)}")

    # Store parsed arguments in the application environment for :codecrafters_redis
    :ok = Application.put_env(:codecrafters_redis, :cli_options, parsed_args, persistent: true)
    IO.puts("CLI.main: Stored parsed_args in application environment for :codecrafters_redis under :cli_options.")
    # IO.inspect(Application.get_env(:codecrafters_redis, :cli_options), label: "CLI.main: Value for :cli_options after put_env") # Keep for now

    # Stop the application if it was auto-started, to ensure clean state before setting env for our controlled start
    :ok = Application.stop(:codecrafters_redis)
    IO.puts("CLI.main: Application.stop(:codecrafters_redis) called.")

    # Re-apply env settings, as stopping might clear non-persistent env vars or for safety.
    :ok = Application.put_env(:codecrafters_redis, :cli_options, parsed_args, persistent: true)
    IO.puts("CLI.main: Re-applied parsed_args to application environment for :codecrafters_redis under :cli_options.")
    IO.inspect(Application.get_env(:codecrafters_redis, :cli_options), label: "CLI.main: Value for :cli_options after re-put_env")


    # Start the application. This should now call Server.start, which will read the env.
    Application.start(:codecrafters_redis, :permanent) # Start with :permanent restart type
                                                                    # This will result in Server.start(:normal, [])
                                                                    # because of `mod: {Server, []}` in mix.exs
    IO.puts("CLI.main: Application.start(:codecrafters_redis) completed.")

    # Mantiene el proceso principal en ejecución
    IO.puts("CLI.main: Entering sleep loop to keep process alive.")
    Process.sleep(:infinity)
  end

  defp parse_cli_args(args) do
    # Parses command line arguments into a map
    switches = [dir: :string, dbfilename: :string]
    # Use OptionParser to parse the command line arguments
    case OptionParser.parse(args, switches: switches) do
      {parsed_options, _remaining_args, []} ->
        # If parsing is successful, convert the parsed options to a map
        config_map = Map.new(parsed_options)
        # Ensure the config_map has the required keys
        config_map
        # If the required keys are not present, we can set defaults or raise an error
      {_parsed_options, _remaining_args, invalid_options} ->
        IO.puts("Error: Invalid command line options: #{inspect(invalid_options)}")
        System.halt(1)
    end
  end
end
