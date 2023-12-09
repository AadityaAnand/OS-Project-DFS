import socket
import os
import multiprocessing
import random

HOST = '127.0.0.1'
PORT_START = 8081
NUM_SERVERS = 3  # Adjust the number of servers as needed

files_directory = 'files'

if not os.path.exists(files_directory):
    os.makedirs(files_directory)

def handle_request(request, filename, content=None):
    try:
        file_path = os.path.join(files_directory, filename)

        if request == 'CREATE':
            if not os.path.exists(file_path):
                with open(file_path, 'w'):
                    pass  # Create an empty file
                return 'File created successfully'
            else:
                return 'File with the same name already exists. Choose a different name.'

        elif request == 'READ':
            with open(file_path, 'r') as file:
                file_content = file.read()
            return file_content

        elif request == 'WRITE':
            if content is not None:
                with open(file_path, 'a') as file:
                    file.write(content)
                return f'Content written to {filename}'
            else:
                return 'No content provided for writing'

        elif request == 'DELETE':
            os.remove(file_path)
            return f'{filename} deleted successfully'

        elif request == 'SEEK':
            file_list = '\n'.join(os.listdir(files_directory))
            return f'Files in {files_directory}:\n{file_list}'

        else:
            return 'Invalid request'

    except FileNotFoundError:
        return 'File not found'

def server_process(port, server_id):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, port))
        s.listen()

        while True:
            conn, addr = s.accept()
            with conn:
                data = conn.recv(1024)
                if not data:
                    break

                request_data = data.decode('utf-8').split('|')
                request = request_data[0]
                filename = request_data[1]
                content = request_data[2] if len(request_data) == 3 else None

                response = handle_request(request, filename, content)
                conn.sendall(response.encode('utf-8'))

if __name__ == '__main__':
    processes = []

    # Generate random server IDs
    server_ids = list(range(NUM_SERVERS))
    random.shuffle(server_ids)

    # Identify the primary server
    primary_server_id = max(server_ids)
    print(f"Primary server ID: {primary_server_id}")

    for i in range(1, NUM_SERVERS):
        port = PORT_START + i
        process = multiprocessing.Process(target=server_process, args=(port, server_ids[i]))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
