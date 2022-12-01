import socket

HEADER = 2048
PORT = 5050

DISCONNECT_MSG = "!DISCONNECT"
# SERVER = socket.gethostbyname(socket.gethostname())
SERVER = "10.20.205.93"
ADDR = (SERVER, PORT)
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)
print("reached until connection.. ")
PRODUCER_MSG = '#Producer'
CONSUMER_MSG = '#Consumer'
TOPIC_MSG = '#Topic'

def send2(topic, msg):
    topic = topic.encode('utf-8')
    message = msg.encode('utf-8')
    client.send(topic)
    client.send(message)
    print(client.recv(2048).decode('utf-8'))

def send(msg):
    message = msg.encode('utf-8')
    # msg_length = len(message)
    # send_length = str(msg_length).encode('utf-8')
    # send_length += b' ' * (HEADER - len(send_length))
    # client.send(send_length)
    client.send(message)
    print("FINISHED CLIENT.SEND(")
    print(client.recv(2048).decode('utf-8'))

# send("Hello World!")
# input()s
# send("Hello Everyone!")
# input()
# send("Hello Tim!")
# send(DISCONNECT_MSG)
    
client.send(CONSUMER_MSG.encode('utf-8'))


print("Enter the topic TO CONSUME : ")
topic = str(input())
# client.send(TOPIC_MSG.encode('utf-8'))
# input()
send(topic)
client.send(DISCONNECT_MSG.encode('utf-8'))
# client.send(DISCONNECT_MSG.encode('utf-8'))

# print("Enter the message : ")
# message = str(input())
# send(message)
# client.send(DISCONNECT_MSG.encode('utf-8'))
