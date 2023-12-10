import socket
import json
import os
import random

FILES_FOLDER = 'files'

IPS = [8081,8083,8084]

def send_request(HOST, PORT, request, filename='', content=None):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        data = f'{request}|{filename}|{content}' if content else f'{request}|{filename}'
        s.sendall(data.encode('utf-8'))
        response = s.recv(1024).decode('utf-8')
    return response

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
        secondary_server = {'id': 2, 'port': 8082}
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
        server_socket.bind(('127.0.0.2', 8012))  # Change the port as needed
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

def is_primary():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('127.0.0.1', 8080))
    
    message = "PRIMARY"
    s.sendall(message.encode('utf-8'))
    response = s.recv(1024).decode('utf-8')
    if response:
        response = json.loads(response)

    id = response["id"]
    port = response["port"]

    if str(port) == "8082":
        return True
    else:
        return False

def replication_file(filename):
    if is_primary():
        chunkserver_1,chunkserver_2 = random.sample(IPS,2)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('127.0.0.1', chunkserver_1))
            response = send_request("127.0.0.1", chunkserver_1,'PRIMARY', filename)
            if response:
                response = json.loads(response)

            id = response["id"]
            port = response["port"]
            print(send_request("127.0.0.1", port, 'CREATE',filename))
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('127.0.0.1', chunkserver_2))
            response = send_request("127.0.0.1", chunkserver_2,'PRIMARY', filename)
            if response:
                response = json.loads(response)

            id = response["id"]
            port = response["port"]
            print(send_request("127.0.0.1", port, 'CREATE',filename))

if __name__ == '__main__':
    # Run the server
    server()
