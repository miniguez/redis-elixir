defmodule Server.Store do
  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def get(key) do
    Agent.get_and_update(__MODULE__, fn state ->
      current_time = System.monotonic_time(:millisecond)
      case Map.get(state, key) do
        nil ->
          {nil, state} # Key not found
        {value, expiration_time} ->
          if not is_nil(expiration_time) and current_time >= expiration_time do
            {nil, Map.delete(state, key)} # Key expired
          else
            {value, state}
          end
      end
    end)
  end

  def set(key, value, expiry_ms \\ nil) do
    expiration_time =
      if expiry_ms do
        System.monotonic_time(:millisecond) + expiry_ms
      else
        nil
      end

    Agent.update(__MODULE__, &Map.put(&1, key, {value, expiration_time}))
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

  # When `mod: {Server, app_args_from_mix_exs}` (here `app_args_from_mix_exs` is `[]`)
  # and Application.start(app, type, type_args_list) is called (e.g. Application.start(:app, :normal, keyword_list_args)),
  # Server.start/2 is called as: Server.start(type, type_args_list).
  # The `app_args_from_mix_exs` are IGNORED by `Application.start/3` if `type_args_list` is provided.
  # If Application.start/2 (e.g. `Application.start(:app, :normal)`) is used, then `type_args_list` would effectively be `app_args_from_mix_exs`.
  def start(start_type, start_type_args) do
    IO.puts("Server.start called with start_type: #{inspect(start_type)}, start_type_args: #{inspect(start_type_args)}")

    # Extract the CLI options from start_type_args, which should be a keyword list
    cli_options_map =
      case {start_type, start_type_args} do
        {:normal, args_list} when is_list(args_list) ->
          # Convert keyword list to map. The keys are already atoms (e.g. :dir, :dbfilename)
          # as produced by Keyword.from_map() and OptionParser
          Map.new(args_list)
        _ ->
          IO.puts("Warning: Server started with unexpected start_type or start_type_args. Using empty map for cli_options.")
          IO.puts("Expected {:normal, keyword_list_args}.")
          %{} # Default to empty map
      end
    IO.puts("Server.start converted CLI options to map: #{inspect(cli_options_map)}")

    children = [
      {Server.Config, cli_options_map}, # Pass the map to Server.Config.start_link/1
      {Task, fn -> Server.listen() end},
      Server.Store
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
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
        # Decide how to handle fatal error, e.g., exit or raise
        # For an OTP application, this worker might crash and be restarted by supervisor
        # depending on strategy. Here, we'll let it crash by re-raising or exiting.
        {:error, reason} # Or System.halt(1) if this is a critical failure point for the app
    end
  end

  defp accept_connections(socket) do
    # Acepta una nueva conexión de cliente
    IO.puts("Waiting to accept connection...")
    {:ok, client} = :gen_tcp.accept(socket)
    IO.puts("Connection accepted from client: #{inspect(client)}")
    # Inicia un nuevo proceso para manejar este cliente
    spawn(fn -> handle_client(client) end)
    # Vuelve a escuchar para más conexiones
    accept_connections(socket)
  end

  defp handle_client(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        IO.puts("Received data: #{inspect(data)}")
        # Maneja el comando recibido
        handle_command(client, data)
        # Continúa escuchando más comandos de este cliente
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
      # Si el comando es ECHO, responde con el argumento
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
          true -> # Default for other 5-argument SET commands that are not PX
            :gen_tcp.send(client, "-ERR syntax error\r\n")
        end
      ["SET", key, value] ->
        Server.Store.set(key, value) # Defaults to no expiry
        :gen_tcp.send(client, "+OK\r\n")
      # Si el comando es GET, recupera el valor y responde
      ["GET", key] ->
        case Server.Store.get(key) do
          nil ->
            :gen_tcp.send(client, "$-1\r\n") # Key not found
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
            # According to Redis spec for `CONFIG GET`, if a parameter doesn't exist,
            # it returns an empty list. However, the problem implies we only get valid ones.
            # For robustness, if it's not one of the two, what to do?
            # The problem says: "the tester will only send CONFIG GET commands with one parameter at a time."
            # and "Your server must respond to each CONFIG GET command with a RESP array containing two elements"
            # This implies we don't need to handle "parameter not found" by returning empty array,
            # but rather that it *will* be found.
            # If it *could* be an unknown param, an empty array `*0\r\n` might be more correct for `CONFIG GET`.
            # However, the example response is `*2...`, so we must provide a value.
            # Let's assume for now that only "dir" and "dbfilename" will be queried.
            # If an unknown parameter *is* queried and Server.Config.get returns nil:
            IO.puts("Warning: CONFIG GET for unknown parameter '#{param_name_input}' (fetching as '#{param_name_to_fetch}')")
            # Based on typical Redis behavior for `CONFIG GET <non-existent-param>`, it returns an empty array.
            # But the problem statement's example response is specific: *2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
            # This implies we don't need to return *0\r\n for unknown params in this stage.
            # Let's send an error if it's not "dir" or "dbfilename" to be safe, or rely on tests only sending valid ones.
            # Given the constraints, if `Server.Config.get` returns nil for "dir" or "dbfilename", something is wrong.
            # For this stage, we'll assume valid parameters are always queried.
            # If `Server.Config.get` returns `nil` for "dir" or "dbfilename", it means they weren't set.
            # This would be an internal error state.
            # The problem implies these values will always be there.
             :gen_tcp.send(client, "-ERR parameter '#{param_name_input}' not configured or supported\r\n")
          value when is_binary(value) ->
            # Respond with a RESP array of two bulk strings: param_name_input and value
            # The problem asks for the *original* parameter name in the response, not necessarily the lowercased one.
            # Example: if input is "DIR", response should be "DIR" and its value.
            response = "*2\r\n$#{String.length(param_name_input)}\r\n#{param_name_input}\r\n$#{String.length(value)}\r\n#{value}\r\n"
            :gen_tcp.send(client, response)
          _ ->
            # This case should ideally not be reached if config is set up correctly.
            :gen_tcp.send(client, "-ERR internal error retrieving config for '#{param_name_input}'\r\n")
        end
      # Para cualquier otro comando, responde con un error
      _ ->
        IO.puts("Unknown command raw data: #{inspect(data)}")
        IO.puts("Unknown command parsed structure: #{inspect(parsed_command)}")
        :gen_tcp.send(client, "-ERR unknown command\r\n")
    end
  end

  defp parse_resp(data) do
    case data do
      # Si los datos comienzan con *, es un comando en formato RESP
      <<?*, count_char::binary-size(1), "\r\n", rest::binary>> ->
        case String.to_integer(count_char) do
          count_int when is_integer(count_int) and count_int >= 0 ->
            parse_bulk_string_array(count_int, rest, [])
          _ ->
            IO.puts("Invalid array count: #{count_char}")
            ["INVALID_RESP_COUNT"] # Error case
        end
      # De lo contrario, trata los datos como una cadena simple (inline command)
      _ ->
        # This path is typically for inline commands like "PING" sent by telnet
        # For RESP arrays, it might indicate an issue or a non-array command format
        trimmed_data = String.trim(data)
        if String.length(trimmed_data) > 0 do
          String.split(trimmed_data, " ") # Split by space for simple commands
        else
          [""] # Handle empty command case
        end
    end
  end

  # Parses a RESP array of bulk strings
  defp parse_bulk_string_array(0, _rest, acc), do: Enum.reverse(acc)
  defp parse_bulk_string_array(_count, <<>>, acc) do
    IO.puts("Reached end of data unexpectedly while parsing bulk string array")
    Enum.reverse(acc) # Or handle as an error
  end
  defp parse_bulk_string_array(count, <<?$, rest_after_dollar::binary>>, acc) do
    # Expecting <length>\r\n<data>\r\n
    case String.split(rest_after_dollar, "\r\n", parts: 2) do
      [length_str, rest_after_length_crlf] ->
        case String.to_integer(length_str) do
          len when is_integer(len) and len >= 0 ->
            if byte_size(rest_after_length_crlf) >= len + 2 do # +2 for the final \r\n
              <<string_data::binary-size(len), "\r\n", rest_after_string_crlf::binary>> = rest_after_length_crlf
              parse_bulk_string_array(count - 1, rest_after_string_crlf, [string_data | acc])
            else
              IO.puts("Insufficient data for string of length #{len}")
              # This is an error condition, data is shorter than expected
              Enum.reverse(["ERROR_INSUFFICIENT_DATA" | acc])
            end
          _ ->
            IO.puts("Invalid bulk string length: #{length_str}")
            Enum.reverse(["ERROR_INVALID_LENGTH" | acc]) # Error case
        end
      _ ->
        IO.puts("Malformed bulk string: missing CRLF after length")
        Enum.reverse(["ERROR_MALFORMED_BULK_STRING" | acc]) # Error case
    end
  end
  defp parse_bulk_string_array(_count, other_data, acc) do # _count was count
    IO.puts("Unexpected data format in parse_bulk_string_array. Expected '$', got: #{inspect other_data}")
    # This signifies a parsing error or unexpected format.
    # Depending on strictness, could return an error token or try to salvage.
    # For now, returning what we have plus an error marker.
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

    # Start the main application, passing the parsed arguments
    # We will need to modify Application.start and Server.start to accept these.
    # For now, we are just parsing. The actual passing will happen in a later step.
    # Start the application, passing the parsed arguments.
    # The :normal type is typical for standard application starts.
    # Convert map to keyword list for Application.start/3's type_args
    type_args = Enum.into(parsed_args, [])
    IO.puts("CLI.main: Starting application with type=:normal, type_args=#{inspect(type_args)}")
    Application.start(:codecrafters_redis, :normal)


    # Mantiene el proceso principal en ejecución
    IO.puts("CLI.main: Entering sleep loop to keep process alive.")
    Process.sleep(:infinity)
  end

  defp parse_cli_args(args) do
    # Define the switches (options) that your CLI will accept.
    # :string indicates that the option takes a string value.
    switches = [dir: :string, dbfilename: :string]
    # Aliases can map shorter options to longer ones, e.g., d: :dir.
    # We don't have aliases specified in the requirements.

    # OptionParser.parse returns a tuple: {parsed_options, remaining_args, invalid_options}
    # For example: --dir /tmp --dbfilename dump.rdb
    # parsed_options would be [dir: "/tmp", dbfilename: "dump.rdb"]
    # remaining_args would be any arguments not matching the defined switches.
    # invalid_options would be any options passed that are not defined or have incorrect types.

    case OptionParser.parse(args, switches: switches) do
      {parsed_options, _remaining_args, []} ->
        # Successfully parsed, no invalid options.
        # We can convert the keyword list to a map for easier access if needed,
        # or keep it as a keyword list.
        # For now, we'll store them as a map.
        config_map = Map.new(parsed_options)

        # Provide default values if not specified, though problem implies they will be.
        # The problem statement says "The tester will execute your program like this:
        # ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb"
        # So, we can assume they will be present.
        # If we needed defaults:
        # Map.get(config_map, :dir, "/default/dir")
        # Map.get(config_map, :dbfilename, "default_dump.rdb")
        config_map

      {_parsed_options, _remaining_args, invalid_options} ->
        IO.puts("Error: Invalid command line options: #{inspect(invalid_options)}")
        # Exit or handle error appropriately
        System.halt(1) # Exit if invalid options are provided
    end
  end
end
