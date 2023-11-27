import socket
import os
import threading

# Server configuration
SERVER_HOST = "192.168.56.1"
SERVER_PORT = 12345
DATA_DIR = "server_data/"

# Create the data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

BUFFER_SIZE = 4096

def list_files(client_socket):
    files = os.listdir(DATA_DIR)
    response = "\n".join(files)
    client_socket.send(response.encode())

def download_file(client_socket, filename):
    file_path = os.path.join(DATA_DIR, filename)

    if os.path.isfile(file_path):
        with open(file_path, "rb") as file:
            data = file.read()
            client_socket.send(data)
    else:
        client_socket.send(b"File not found")

def upload_file(client_socket, filename):
    data = client_socket.recv(BUFFER_SIZE)
    file_path = os.path.join(DATA_DIR, filename)

    with open(file_path, "wb") as file:
        file.write(data)
        client_socket.send(b"File uploaded successfully")

def delete_file(client_socket, filename):
    file_path = os.path.join(DATA_DIR, filename)

    if os.path.isfile(file_path):
        os.remove(file_path)
        client_socket.send(b"File deleted successfully")
    else:
        client_socket.send(b"File not found")

def handle_client(client_socket):
    request = client_socket.recv(BUFFER_SIZE).decode()
    command, *params = request.split()

    if command == "LIST":
        list_files(client_socket)

    elif command == "DOWNLOAD":
        filename = params[0]
        download_file(client_socket, filename)

    elif command == "UPLOAD":
        filename = params[0]
        upload_file(client_socket, filename)

    elif command == "DELETE":
        filename = params[0]
        delete_file(client_socket, filename)

    # client_socket.close()

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((SERVER_HOST, SERVER_PORT))
    server.listen(5)

    print(f"Server listening on {SERVER_HOST}:{SERVER_PORT}")

    try:
        while True:
            client_socket, _ = server.accept()
            print("Accepted connection from:", client_socket.getpeername())
            client_handler = threading.Thread(target=handle_client, args=(client_socket,))
            client_handler.start()

    except KeyboardInterrupt:
        print("Server shutting down.")
    finally:
        server.close()

if __name__ == "__main__":
    main()
