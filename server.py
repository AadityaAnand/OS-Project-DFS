import socket
import threading

# it will dynamically get the ip address
HOST = socket.gethostbyname(socket.gethostname())
# HOST = '192.168.56.1'
PORT = 9099

#hsndle client function
def handle_client(communcation_socket, address):
    print(f'[New Connection] {address} connected.')
    # Initialized a new connection
    connected = True
    while connected:
        message = communcation_socket.recv(1024).decode('utf-8')
        # If the clinets wants to disconnect then we need to update the connected to false.
        if message == "disconnect":
            connected = False
        
        print(f"[{address}] {message}")
        # sending back a message to client that messsage has been received.
        communcation_socket.send(f"Got your Message! Thank You!".encode('utf-8'))   

    communcation_socket.close()



def main():
    print('Server is starting')
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen() 
    # it will listen for any clients to connect.

    print(f'Server is listing on {HOST},{PORT}')

    while True:
        # To accept the connect from the client.
        communication_socket, adddress = server.accept()
        thread = threading.Thread(target=handle_client, args=(communication_socket,adddress))
        thread.start()
        # to know how many clients are there
        print(f"[Active Connections] {threading.activeCount() - 1}")


if __name__ == "__main__":
    main()


# while True:
#     communication_socket, adddress = server.accept()
#     print("Connected to {address}")
#     message = communication_socket.recv(1024).decode('utf-8')
#     print(f"message from clinet is: {message}")
#     communication_socket.send(f"Got your Message! Thank You!".encode('utf-8'))
#     communication_socket.close()
#     print("conection with {address} ended")