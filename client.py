import socket
import os

# Client configuration
SERVER_HOST = "192.168.56.1"
SERVER_PORT = 12345
BUFFER_SIZE = 4096

def receive_data_and_save(client, filename):
    with open(filename, "wb") as file:
        while True:
            data_chunk = client.recv(BUFFER_SIZE)
            if not data_chunk:
                break
            file.write(data_chunk)

def send_file_data(client, filename):
    with open(filename, "rb") as file:
        data = file.read()
        client.sendall(data)

def main():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((SERVER_HOST, SERVER_PORT))

    try:
        while True:
            command = input("Enter a command (LIST, DOWNLOAD, UPLOAD, DELETE, QUIT): ").strip()
            client.sendall(command.encode())

            if command == "LIST":
                file_list = client.recv(BUFFER_SIZE).decode()
                print("Available files:")
                print(file_list)

            elif command.startswith("DOWNLOAD"):
                filename = command.split(maxsplit=1)[1].strip()
                receive_data_and_save(client, filename)
                print(f"Downloaded {filename}")

            elif command.startswith("UPLOAD"):
                filename = command.split(maxsplit=1)[1].strip()
                send_file_data(client, filename)
                response = client.recv(BUFFER_SIZE).decode()
                print(response)

            elif command.startswith("DELETE"):
                filename = command.split(maxsplit=1)[1].strip()
                response = client.recv(BUFFER_SIZE).decode()
                print(response)

            elif command == "QUIT":
                break

    finally:
        client.close()

if __name__ == "__main__":
    main()
