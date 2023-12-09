import socket, json

#HOST = '127.0.0.1'
#PORT = 8080  # Match the metadata server's port

def send_request(HOST, PORT, request, filename='', content=None):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        data = f'{request}|{filename}|{content}' if content else f'{request}|{filename}'
        s.sendall(data.encode('utf-8'))
        response = s.recv(1024).decode('utf-8')
    return response

def menu():
    print("Choose an option:")
    print("1. Create a file")
    print("2. Read from a file")
    print("3. Write to a file")
    print("4. Delete a file")
    print("5. Seek in a file")
    print("6. Quit")

while True:
    menu()
    choice = input("Enter your choice (1-6): ")

    if choice == '1':
        filename = input("Enter the filename to create: ")
        response = send_request("127.0.0.1", 8080,'CREATE', filename)
        if response:
            response = json.loads(response)
            print(response)
        id = response.primary_server_id
        port = response.port

        print(send_request(id, port, 'CREATE',filename))


    elif choice == '2':
        filename = input("Enter the filename to read: ")
        response = send_request('READ', filename)
        print(response)

    elif choice == '3':
        filename = input("Enter the filename to write to: ")
        content = input("Enter the content to write: ")
        response = send_request('WRITE', filename, content)
        print(response)

    elif choice == '4':
        filename = input("Enter the filename to delete: ")
        response = send_request('DELETE', filename)
        print(response)

    elif choice == '5':
        response = send_request('SEEK')
        print(response)

    elif choice == '6':
        print("Exiting the client.")
        break

    else:
        print("Invalid choice. Please enter a number between 1 and 6.")

    another_operation = input("Do you want to perform another operation? (yes/no): ")
    if another_operation.lower() != 'yes':
        print("Exiting the client.")
        break
