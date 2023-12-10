import socket
import json
import os

FILES_FOLDER = 'files'

def handle_client_request(client_socket, request_data):
    # Extract information from the client's request
    print(request_data)
    data = request_data.split("|")
    if len(data) == 2:
        request, filename = data
    else:
        request, filename, content = data

    if request == 'PRIMARY':
        # For simplicity, always return a fixed secondary server ID and port
        secondary_server = {'id': 4, 'port': 8084}
        client_socket.sendall(json.dumps(secondary_server).encode('utf-8'))
    elif request == 'CREATE':
        response = create_file(filename)
        client_socket.sendall(response.encode('utf-8'))
    elif request == 'READ':
        response = read_file(filename)
        client_socket.sendall(response.encode('utf-8'))
    elif request == 'WRITE':
        print("write")
        response = write_to_file(filename, content=content)
        client_socket.sendall(response.encode('utf-8'))
    elif request == 'DELETE':
        response = delete_file(filename)
        client_socket.sendall(response.encode('utf-8'))
    elif request == 'LIST':
        response = list_files()
        client_socket.sendall(response.encode('utf-8'))
    else:
        # Invalid request
        client_socket.sendall("Invalid request".encode('utf-8'))

def create_file(filename, content=""):
    try:
        with open(os.path.join(FILES_FOLDER, filename), 'w') as file:
            file.write(content)
        return f"File '{filename}' created on server with content: {content}"
    except Exception as e:
        return f"Error creating file: {str(e)}"

def read_file(filename):
    try:
        with open(os.path.join(FILES_FOLDER, filename), 'r') as file:
            content = file.read()
        return f"Content read from file '{filename}': {content}"
    except FileNotFoundError:
        return f"File '{filename}' not found on server"
    except Exception as e:
        return f"Error reading file: {str(e)}"

def write_to_file(filename, content):
    try:
        with open(os.path.join(FILES_FOLDER, filename), 'a') as file:
            file.write(content)
        return f"Content written to file '{filename}': {content}"
    except FileNotFoundError:
        return f"File '{filename}' not found on server"
    except Exception as e:
        return f"Error writing to file: {str(e)}"

def delete_file(filename):
    try:
        file_path = os.path.join(FILES_FOLDER, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            return f"File '{filename}' deleted on server"
        else:
            return f"File '{filename}' not found on server"
    except Exception as e:
        return f"Error deleting file: {str(e)}"

def list_files():
    try:
        files_list = os.listdir(FILES_FOLDER)
        return f"Files in 'files' folder: {', '.join(files_list)}"
    except Exception as e:
        return f"Error listing files: {str(e)}"

def server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(('127.0.0.1', 8012))  # Change the port as needed
        _, port = server_socket.getsockname()
        server_socket.listen()

        print(f"Server is listening on port {port} for client connections...")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Connected to client: {client_address}")

            # Receive the client's request data
            request_data = client_socket.recv(1024).decode('utf-8')

            # Handle the client's request
            handle_client_request(client_socket, request_data)

if __name__ == '__main__':
    # Run the server
    server()
