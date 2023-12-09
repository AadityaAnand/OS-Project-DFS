import socket
import json
import random
import os

METADATA_FILE = 'metadata.json'
NUM_SERVERS = 3
PORT_START = 8080

def get_primary_server():
    # Read metadata from the JSON file
    with open(METADATA_FILE, 'r') as file:
        metadata_list = json.load(file)

    # Select the primary server with the highest server ID
    primary_server_id = max([server['id'] for server in metadata_list]) if metadata_list else 1
    return {'id': primary_server_id, 'port': PORT_START + primary_server_id - 1}

def save_metadata(metadata):
    # Read existing metadata from the JSON file
    with open(METADATA_FILE, 'r') as file:
        metadata_list = json.load(file)

    # Update metadata list
    metadata_list.append(metadata)

    # Write updated metadata back to the JSON file
    with open(METADATA_FILE, 'w') as file:
        json.dump(metadata_list, file, indent=2)

def handle_client_request(client_socket, client_address):
    # Get primary server information
    primary_server = get_primary_server()

    # Send primary server information to the client
    client_socket.sendall(json.dumps(primary_server).encode('utf-8'))

    # Receive metadata from the client
    metadata = client_socket.recv(1024).decode('utf-8')
    metadata = json.loads(metadata)

    # Save metadata to the JSON file
    save_metadata(metadata)

    print(f"Metadata received and stored: {metadata}")

    # Debug statement to print the current metadata list
    print("Current metadata list:", metadata)

def initialize_metadata_file():
    # Initialize metadata file if it doesn't exist
    if not os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'w') as file:
            json.dump([], file)

def metadata_server():
    initialize_metadata_file()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(('127.0.0.1', 8081))  # Bind to a dynamically assigned port
        _, port = server_socket.getsockname()
        server_socket.listen()

        print(f"Metadata Server is listening on port {port} for client connections...")
        print(f"Metadata Directory: {os.path.abspath(METADATA_FILE)}")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Connected to client: {client_address}")

            # Print metadata connection details
            print(f"Metadata Server Connection Details:")
            print(f"  IP Address: {'127.0.0.1'}")
            print(f"  Port: {port}")

            # Handle client request
            handle_client_request(client_socket, client_address)

if __name__ == '__main__':
    metadata_server()
