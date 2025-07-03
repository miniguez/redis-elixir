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

defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    children = [
      {Task, fn -> Server.listen() end},
      Server.Store # Add the store to the supervision tree
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
  def main(_args) do
    # Inicia la aplicación del servidor
    {:ok, _pid} = Application.ensure_all_started(:codecrafters_redis)

    # Mantiene el proceso principal en ejecución
    # This is only needed if CLI.main is the entry point of the entire VM
    # and not if this is just a utility to ensure the app is running.
    IO.puts("CLI.main: Entering sleep loop to keep process alive.")
    Process.sleep(:infinity)
  end
end
