from server import DHT
import dht_pb2 as dht
import dht_pb2_grpc as rpc
from client import Client
import time
import threading
import logging
import sys
import os
import grpc

addr = 'localhost:'

portList = {}
threadList = []
nodes = []
nextPort = 50054


def Loading(tot_num, curr_num, nn):
    # Base cond only run when calling the first time
    if nn == 0:
        print("Why are we here?")

    sys.stdout.write('\x1b[1A')
    sys.stdout.write('\x1b[2K')

    load_num = 35

    yy = (curr_num*load_num)/(tot_num-1)
    yy = int(yy)
    topr = ""
    for k in range(load_num):
        if k < yy:
            # topr = topr + "🤍"
             topr = topr + "*"
            # topr = topr + '🕺'
        else:
            # topr = topr + "🖤"
            topr = topr + " "
            # topr = topr + '💃'

    print(topr)


def Load_For_Sec(seconds, header_t="", tailer_t=""):
    # print("=========================================================================")
    # if header_t[0] == 'T' and header_t[1]=='h':
    #     header_t=""
    if header_t:
        print(header_t)
    nn = 0
    start_time = time.time()
    while (time.time()-start_time) < seconds:
        Loading(seconds, time.time()-start_time, nn)
        nn += 1
    if tailer_t:
        print("-------------------------------------------------------------------------")
        print(tailer_t)
    print("==========================================================================")


def userPrompt():
    print('Enter 1 for adding a node to the network')
    print('Enter 2 for removing an existing node from the network')
    print('Enter 3 for adding a key-value pair to the network')
    print('Enter 4 for retrieving a value from the network')
   
    divide()


def startServer(address):
    s = DHT(address)


def divide():
    print()
    print("==========================================================================")
    print()


def createNetwork():
    t1 = threading.Thread(target=startServer, args=('localhost:50051',))
    t1.start()

    t2 = threading.Thread(target=startServer, args=('localhost:50052',))
    t2.start()

    t3 = threading.Thread(target=startServer, args=('localhost:50053',))
    t3.start()

    threadList.append(t1)
    threadList.append(t2)
    threadList.append(t3)

    #  time.sleep(5)
    Load_For_Sec(5, "Creating Nodes")
    c1 = Client('localhost:50051')
    c2 = Client('localhost:50052')
    c3 = Client('localhost:50053')

    c1.join(c2.address)
    # time.sleep(2)
    Load_For_Sec(4, "Joining Node 1")
    c2.join(c3.address)
    # time.sleep(2)
    Load_For_Sec(4, "Joining Node 2")
    c3.join(c1.address)
    # time.sleep(2)
    Load_For_Sec(4, "Joining Node 3")

    nodes.append(c1)
    nodes.append(c2)
    nodes.append(c3)
    portList['50051'] = 1
    portList['50052'] = 1
    portList['50053'] = 1

    print('Network Created')
    divide()


def addNode():

    port = input('Enter port on which you want to add new node: ')
    while port in portList.keys():
        port = input('Port already in use.Enter another:')

    address = addr+str(port)
    t = threading.Thread(target=startServer, args=(address,))
    t.start()

    Load_For_Sec(2, "Creating new node")
    c = Client(address)
    try:
        c.join(nodes[0].address)
    except grpc.RpcError as e:
        logging.info('Error', e.details())
    else:
        nodes.append(c)
        # time.sleep(5)
        Load_For_Sec(10, "Adding node to the network")
        portList[port] = 1
        print('Node added to the network')
        divide()


def removeNode():
    val = input('Enter the port for the node you want to remove: ')
    if val in portList.keys():
        address = addr+val
        c = Client(address)
        try:
            c.leave(c.node_info)
        except grpc.RpcError as e:
            logging.info('Error', e.details())
        else:
            # print(c)
            for i in range(0, len(nodes)):
                if c.node_info == nodes[i].node_info:
                    # print(c)
                    nodes.pop(i)
                    break
            # time.sleep(5)
            Load_For_Sec(10, "Removing node from the network")
            del portList[val]
            print('Node removed successfully')
            divide()
    else:
        print("Node does not exist")
        divide()


def putData():
    key = input('Enter key:')
    value = input('Enter value: ')

    data = dht.KeyValue(key=key, value=value)
    try:
        nodes[0].store(data)
    except grpc.RpcError as e:
        logging.info('Error', e.details())
    else:
        Load_For_Sec(
            5, "This happens instantly but added it just so you can think about your life")
        print('Added data to the network')
        divide()


def getData():
    key = input('Enter key:')
    data = dht.KeyValue(key=key)

    try:
        response = nodes[0].retrieve(data)
    except grpc.RpcError as e:
        logging.info('Error', e.details())
    else:
        Load_For_Sec(
            3, "This ALSO happens instantly but you should think some more")
        if response.value == '':
            print('Key not in the network')
            divide()
        else:
            print('Value:', response.value)
            divide()


def userInput(inp):
    if inp == 1:
        addNode()
    elif inp == 2:
        removeNode()
    elif inp == 3:
        putData()
    elif inp == 4:
        getData()
    else:
        print('Enter a value between 1-4 as per the choices')
        divide()


def main():

    logging.basicConfig(filename='logs.log', level=logging.INFO,
                        filemode='a', format='%(asctime)s %(message)s')
    createNetwork()

    userPrompt()

    while True:
        inp = int(input('Enter a value: '))
        
        userInput(inp)
        userPrompt()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Interrupted')
        try:
            # stopThreads()
            sys.exit(130)
        except SystemExit:
            # stopThreads()
            os._exit(130)
            
            
