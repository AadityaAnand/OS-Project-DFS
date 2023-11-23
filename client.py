import socket
import sys

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
DISSCONNECT_MESSAGE = "!DISCONNECT"
SERVER = "192.168.56.1"
ADDR = (SERVER, PORT)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    print(client.recv(2048).decode(FORMAT))

send("Hello World!")
input()

def read_data_from_minion():
    return None

# def get()
def retrieve_file_data(master, filename, enable_debug=False):
    file_entry = master.fetch_file_entry(filename)

    if not file_entry:
        print ("File not found in the list")
        return
    
    for block_info in file_entry:
        if enable_debug:
            print("Debug Info:", block_info)

        for minion_id in [master.get_minions()[_] for _ in block_info[1]]:
            data_chunk = read_data_from_minion(block_info[0], minion_id)

            if data_chunk:
                sys.stdout.write(data_chunk)
                break
        
        else:
            print("missing data in file Block")

    


if __name__ == "__main__":
    main(sys.argv[1:])