import socket
import threading
import time

HEADER = 2048
PORT = 5050
SERVER = "10.20.205.93"
# SERVER = socket.gethostbyname(socket.gethostname())
# print(SERVER)
ADDR = (SERVER, PORT)
PRODUCER_MSG = '#Producer'
CONSUMER_MSG = '#Consumer'
DISCONNECT_MSG = "!DISCONNECT"
TOPIC_MSG = '#Topic'

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

def getALL(topic,dict):
     list = dict[topic]
     string = ""
     for i in range(len(list)):
          string += list[i] + " "
    
     return string


dict = {}

def producerFunc(conn, addr):
    #  topicVar
    print(f"[NEW PRODUCER CONNECTION] {addr} connected.")
    topicVar = ""
    connected = True
    while connected:
       msg = conn.recv(HEADER).decode('utf-8')
       if msg == TOPIC_MSG :
            
            topicVar = conn.recv(HEADER).decode('utf-8')
            print(f"topic recieved : {topicVar}")
            conn.send("Topic received".encode('utf-8'))
            
       
       messagerecieved = conn.recv(HEADER).decode('utf-8')
      
       if topicVar not in dict:
      # topicVar and message are here
          list = []
          dict[topicVar] = list
          dict[topicVar].append(str(messagerecieved))
       
       elif topicVar in dict:
          dict[topicVar].append((messagerecieved))

       print(f"topic : {topicVar} message: {messagerecieved}")
       conn.send(messagerecieved.encode('utf-8'))

       disco = conn.recv(HEADER).decode('utf-8')
       if disco== DISCONNECT_MSG:
                connected = False
    print("DISCONNECT")
    conn.close()

def consumerFunc(conn, addr):
     #  topicVar
    print(f"[NEW CONSUMER CONNECTION] {addr} connected.")
    topicVar = conn.recv(HEADER).decode('utf-8')
    connected = True
    
    
    # messagerecieved = dict[topicVar]
    messagerecieved = getALL(topicVar, dict)
    stri = "Message from the topic " + str(topicVar) +" is: "+ str(messagerecieved)
    conn.send(stri.encode('utf-8'))
    
    
    disco = conn.recv(HEADER).decode('utf-8')
    print("DISCONNECT")
    conn.close()

# def handle_client(conn, addr):
#     print(f"[NEW CONNECTION] {addr} connected.")
#     connected = True
#     while connected:
#         # msg_length = conn.recv(HEADER).decode('utf-8')
#         # if msg_length:
#             # msg_length = int(msg_length)
#             msg = conn.recv(HEADER).decode('utf-8')
#             if msg == PRODUCER_MSG:
#                  t1 = threading.Thread(target=producerFunc(conn, addr))
#                  t1.start()
#             if msg == CONSUMER_MSG:
#                  t2 = threading.Thread(target=consumerFunc)
#                  t2.start()
#             if msg == DISCONNECT_MSG:
#                 connected = False

#             # print(f"[{addr}] {msg} LOL")
#             # conn.send("Msg received".encode('utf-8'))
#     conn.close()SS

def start():
    server.listen()
    print(f"[LISTENING] SERVER IS LISTENING ON {SERVER}")
    while True:
         conn, addr = server.accept()
         msg = conn.recv(HEADER).decode('utf-8')
         print(f"HELLO recieved {msg}")
         if msg == PRODUCER_MSG:
                 t1 = threading.Thread(target=producerFunc, args =(conn, addr))
                 t1.start()
         elif msg == CONSUMER_MSG:
                 t2 = threading.Thread(target=consumerFunc, args =(conn, addr))
                 t2.start()
         print(f"[ACTIVE CONNECTIONS] {threading.active_count()-1}") 

print("[STARTING] Server is starting ...")
start()



# t1 = threading.Thread(target=producer_to_broker)
# t2 = threading.Thread(target=broker_to_consumer)