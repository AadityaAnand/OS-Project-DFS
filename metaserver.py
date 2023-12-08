import socket
import json

METADATA_FILE = 'metadata.json'

def get_primary_server_id():
    # Replace this logic with your actual mechanism to determine the primary server ID
    # For example, you could read it from a shared configuration or a central authority
    server_ids = [1, 2, 3]  # Sample server IDs
    return max(server_ids)

def get_primary_server():
    primary_server_id = get_primary_server_id()
    primary_server_port = 8080 + primary_server_id
    return {'id': primary_server_id, 'port': primary_server_port}

def handle_client_request(client_socket, client_address):
    # Get primary server information
    primary_server = get_primary_server()
    
    # Send primary server information to the client
    client_socket.sendall(json.dumps(primary_server).encode('utf-8'))

    # Receive metadata from the client
    metadata = client_socket.recv(1024).decode('utf-8')
    metadata = json.loads(metadata)

    # Store metadata in a JSON file
    with open(METADATA_FILE, 'a') as metadata_file:
        metadata_file.write(json.dumps(metadata) + '\n')

    print(f"Metadata received and stored: {metadata}")

def metadata_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(('127.0.0.1', 8081))  # Adjust the port as needed
        server_socket.listen()

        print("Metadata Server is listening for client connections...")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Connected to client: {client_address}")

            # Handle client request in a separate thread or process
            handle_client_request(client_socket, client_address)

if __name__ == '__main__':
    primary_server_id = get_primary_server_id()
    print(f"Primary server ID: {primary_server_id}")

    # Start metadata server
    metadata_server()
