defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    # Inicia el servidor como una aplicación supervisada
    Supervisor.start_link([{Task, fn -> Server.listen() end}], strategy: :one_for_one)
  end

  @doc """
  Listen for incoming connections
  """
  def listen() do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    IO.puts("Logs from your program will appear here!")

    # Abre un socket TCP en el puerto 6379 (puerto estándar de Redis)
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])
    accept_connections(socket)
  end

  defp accept_connections(socket) do
    # Acepta una nueva conexión de cliente
    {:ok, client} = :gen_tcp.accept(socket)
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
        IO.puts("Client disconnected")
    end
  end

  defp handle_command(client, data) do
    case parse_resp(data) do
      # Si el comando es PING, responde con PONG
      ["PING"] ->
        :gen_tcp.send(client, "+PONG\r\n")
      # Para cualquier otro comando, responde con un error
      _ ->
        IO.puts("Unknown command: #{inspect(data)}")
        :gen_tcp.send(client, "-ERR unknown command\r\n")
    end
  end

  defp parse_resp(data) do
    case data do
      # Si los datos comienzan con *, es un comando en formato RESP
      <<?*, count::binary-size(1), "\r\n", rest::binary>> ->
        parse_bulk_string(String.to_integer(count), rest, [])
      # De lo contrario, trata los datos como una cadena simple
      _ ->
        String.split(String.trim(data))
    end
  end

  # Función recursiva para parsear strings en formato RESP
  defp parse_bulk_string(0, _, acc), do: Enum.reverse(acc)
  defp parse_bulk_string(count, <<?$, len::binary-size(1), "\r\n", rest::binary>>, acc) do
    # Extrae la cadena de la longitud especificada
    {string, rest} = String.split_at(rest, String.to_integer(len))
    # Continúa parseando el resto de los datos
    parse_bulk_string(count - 1, String.slice(rest, 2..-1//-1), [string | acc])
  end
end

defmodule CLI do
  def main(_args) do
    # Inicia la aplicación del servidor
    {:ok, _pid} = Application.ensure_all_started(:codecrafters_redis)

    # Mantiene el proceso principal en ejecución
    Process.sleep(:infinity)
  end
end
