import socket

# HOST = '192.168.56.1' 

HOST = socket.gethostbyname(socket.gethostname())
PORT = 9099


def main():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))
    print(f"Clinet is Connected to server at {HOST}:{PORT}")

    connected = True
    while connected:
        message = input("> ")

        client.send(message.encode('utf-8'))

        if message == "disconnect":
            connected = False
        else:
            message = client.recv(1024).decode('utf-8')
            print(f"server {message}")



if __name__ == "__main__":
    main()

# socket.send("Hello World!".encode('utf-8'))
# print(socket.recv(1024).decode('utf-8'))