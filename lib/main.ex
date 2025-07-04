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
      {Server.Config, cli_options_map},
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
