import asyncio, pickle,  threading, time, sys
import socket, hashlib
import random, string
class PeerNode:
    def __init__(self, name):
        self.peerName = name
        self.peerID = int(ord(self.peerName)-65)
        self.peerPort = 5000+int(ord(self.peerName))-65
        self.DHT = {}
        self.subscriptions = {}
        self.readHistory = {}
        self.grbCollectIndex = 0
        self.lock = asyncio.Lock()

        print(f"Name : {self.peerName}")
        print(f"ID: {self.peerID}")
        print(f"Port: {self.peerPort}")

    def garbageCollector(self, topic):
        #check if everyone read the messages
        grbCollectIndex = len(self.DHT[topic])

        for key in self.readHistory.keys():
            #pick up the same topic
            if key[0] == topic and grbCollectIndex != 0:
                grbCollectIndex = min(grbCollectIndex, self.readHistory[key])
                                
        if grbCollectIndex == len(self.DHT[topic]) and len(self.DHT[topic]) !=0:
            self.DHT[topic] = []    #garbage collect 
            #set all indexes to 0 after garbage collected
            for key in self.readHistory.keys():
                if key[0] == topic:
                    self.readHistory[key] = 0
            print(f"Garbage collected!")

    async def write_to_file(self, message):
        async with self.lock:
            with open(f'peer_{self.peerName}.log', 'a') as file:
                file.write(f"\n[{time.ctime().split(' ')[4][:8]}] : {message}")

    async def hashTheTopic(self, topic):    
        #computes the hash value of encoded topic
        hash_object = hashlib.sha256(topic.encode())
        #convert to hexadecimal
        hash_int = int(hash_object.hexdigest(), 16)
        #takes the modulo of 8(Nodes)
        node_index = hash_int % 8
        return node_index
    
    async def findHypercubeRoute(self, fromNode, toNode):
        #initialize the source and destination
        callingNode = format(fromNode, 'b').zfill(3)  
        targetNode = format(toNode, 'b').zfill(3)
        
        print(f"Source: {callingNode}, Destination: {targetNode}")
        
        # Store the route
        route = []
        
        current = fromNode
        #while we have'nt found the destination node
        while current != toNode:
            route.append(current)   #append current node
           
            for i in range(len(callingNode)):
                if (current >> i) & 1 != (toNode >> i) & 1: #shifts current right by i and check the significant bit and check if bits in i differ in node and current
                    current ^= (1 << i)     #if so, flips the bit at i position in current
                    break
        
        route.append(toNode)  
        return route

    async def checkIfNeighbor(self, node):
        print(f"Checking if node is peer.\n")
        print(f"SelfID : {self.peerID}, peerToConnect : {int(node)}\n")
        
        #apply xor to the node and current node
        xor_result = int(self.peerID) ^ int(node)

        #check if there's only 1 bit difference
        res = xor_result != 0 and (xor_result & (xor_result - 1)) == 0
        print(f"Difference is 1 bit? -> {res}")

        if res:
            peerToConnectPort = 5000 + int(node)
            print(f"Neightbor! It's port is {peerToConnectPort}\n")
            return peerToConnectPort
        else:
            print(f"Peer {node} is not neighbor. We need to forward!\n")
            return None

    async def createTopic(self, topic):
        print("\nCreating topic...")
        print("\n#############################################################\n")

        #Check which Node to write
        peerToWrite = await self.hashTheTopic(topic)
        print(f"The topic '{topic}' is assigned to node {peerToWrite}.")
        
        #check if topic has been assinged to node itself.
        if peerToWrite == int(self.peerID):
            if topic in self.DHT:
                print(f"Topic {topic} already exists")
            else:
                self.DHT[topic] = []
                self.subscriptions[topic] = []
            return
        
        #Topic has been assigned to other
        else:
            #calls to calculate the route to destination
            route = await self.findHypercubeRoute(self.peerID, peerToWrite)
            print(f"Route -> {route}\n")
            route = route[1:]
            #removes the first element and assigns the next node in transit
            nextNodeInTransit = route[0]
            reader, writer = await asyncio.open_connection('127.0.1.1', 5000+nextNodeInTransit)
            request = {
                'requestName': 'createTopic', 
                'caller': self.peerName, 
                'peerToWrite' : peerToWrite,
                'route' : route,
                'topic': topic
                }
            
            addToFile = f"[Node {self.peerName} called : {request}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            await self.write_to_file(addToFile)

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)

            addToFile = f"[Response from Peer Node {peerToWrite} : {response}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            
            writer.close()
            await writer.wait_closed()

    async def deleteTopic(self, topic):
        print("\nDeleting topic...")

        #compute the hash and find the assigned node for the topic
        peerToDelete = await self.hashTheTopic(topic)

        #check if topic has been assinged to node itself.
        if peerToDelete == int(self.peerID):
            #if topic exist in DHT, then delete
            if topic in self.DHT:
                print(f"Topic {topic} already delete")
                del self.DHT[topic]
                del self.subscriptions[topic]
            else:
                print(f"Topic does not exist\n")
            return
        else:
            #if it belongs to other node, find the route and forward the request
            route = await self.findHypercubeRoute(self.peerID, peerToDelete)
            print(f"Route -> {route}\n")
            route = route[1:]
            nextNodeInTransit = route[0]
            reader, writer = await asyncio.open_connection('127.0.1.1', 5000+nextNodeInTransit)
            request = {
                'requestName': 'deleteTopic', 
                'caller': self.peerName, 
                'peerToWrite' : peerToDelete,
                'route' : route,
                'topic': topic
                }
            
            addToFile = f"[Node {self.peerName} called : {request}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            await self.write_to_file(addToFile)

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)

            addToFile = f"[Response: {response}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            await self.write_to_file(addToFile)
            
            writer.close()
            await writer.wait_closed()

    async def send(self, topic, message):
        print(f"\nSending message {message} to {topic}\n")

        #Check which Node to write
        peerToWrite = await self.hashTheTopic(topic)
        print(f"The topic '{topic}' is assigned to node {peerToWrite}.")
        
        #check if topic has been assinged to node itself.
        if peerToWrite == int(self.peerID):
            if topic in self.DHT:
                self.DHT[topic].append(message)
            else:
                print(f"Topic {topic} does not exist to send message!")
        else:
            #if it belongs to other node, find the route and forward the request
            route = await self.findHypercubeRoute(self.peerID, peerToWrite)
            print(f"Route -> {route}\n")
            route = route[1:]
            nextNodeInTransit = route[0]
            reader, writer = await asyncio.open_connection('127.0.1.1', 5000+nextNodeInTransit)
            request = {
                'requestName': 'send', 
                'caller': self.peerName, 
                'peerToWrite' : peerToWrite,
                'route' : route,
                'topic': topic,
                'message' : message
                }
            
            addToFile = f"[Node {self.peerName} called : {request}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            await self.write_to_file(addToFile)

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)

            addToFile = f"[Response: {response}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            await self.write_to_file(addToFile)

            writer.close()
            await writer.wait_closed()
            
    async def subscribe(self, topic):
        print(f"\nSubscribing to {topic} \n")
        print("\n**************************************************************n")

        #Check which Node to write
        peerToWrite = await self.hashTheTopic(topic)
        
        #check if topic has been assinged to node itself.
        if peerToWrite == int(self.peerID):
            print(f"You can't subscribe to your own topic!\n")
        else:
            #if it belongs to other node, find the route and forward the request
            route = await self.findHypercubeRoute(self.peerID, peerToWrite)
            print(f"Route -> {route}\n")
            route = route[1:]
            nextNodeInTransit = route[0]
            reader, writer = await asyncio.open_connection('127.0.1.1', 5000+nextNodeInTransit)
            request = {
                'requestName': 'subscribe', 
                'caller': self.peerName, 
                'peerToWrite' : peerToWrite,
                'route' : route,
                'topic': topic
                }
            
            addToFile = f"[Node {self.peerName} called : {request}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            await self.write_to_file(addToFile)

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)

            addToFile = f"[Response: {response}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            await self.write_to_file(addToFile)

            writer.close()
            await writer.wait_closed()
      
    async def pull(self, topic):

        #Check which Node to write
        peerToWrite = await self.hashTheTopic(topic)
        
        #check if topic has been assinged to node itself.
        if peerToWrite == int(self.peerID):
            print(f"You can't pull from your own topic!\n")
        else:
            #if it belongs to other node, find the route and forward the request
            route = await self.findHypercubeRoute(self.peerID, peerToWrite)
            print(f"Route -> {route}\n")
            route = route[1:]
            nextNodeInTransit = route[0]
            reader, writer = await asyncio.open_connection('127.0.1.1', 5000+nextNodeInTransit)
            request = {
                'requestName': 'pull', 
                'caller': self.peerName, 
                'peerToWrite' : peerToWrite,
                'route' : route,
                'topic': topic
                }
            
            addToFile = f"[Node {self.peerName} called : {request}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            await self.write_to_file(addToFile)

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)

            addToFile = f"[Response: {response}]"
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : {addToFile}")
            await self.write_to_file(addToFile)

            writer.close()
            await writer.wait_closed()

    async def send_messageToPeer(self, peerIP, peerPort, message):
        #connects to the peer
        reader, writer = await asyncio.open_connection(peerIP, peerPort)
        print("\nConnection established with other peer \n")
        
        request = pickle.dumps(message)
        writer.write(request)
        await writer.drain()

        data = await reader.read(2000)
        response = pickle.loads(data)
        print(f"\n[{time.ctime().split(' ')[4][:8]}] : [Node {self.peerName} received reply: {response}]")
        writer.close()
        await writer.wait_closed()
        return response

    async def listen_for_messages(self):
        server = await asyncio.start_server(self.handle_incoming_messages, socket.gethostname(), self.peerPort)  # Bind to port
        localIP = server.sockets[0].getsockname()[0]
        port = server.sockets[0].getsockname()[1]
        print(f"[*] Listening for messages on {localIP} {port}")

        async with server:
            await server.serve_forever()

    async def handle_incoming_messages(self, reader, writer):
        try:
            addr = writer.get_extra_info('peername')
            print(f"\n[{time.ctime().split(' ')[4][:8]}] : [Node {self.peerName} accepted connection from {addr}]")

            while True:
                data = await reader.read(200)
                if not data:
                    break
                dataObject = pickle.loads(data)
                request = dataObject['requestName']
                print(f"\n[{time.ctime().split(' ')[4][:8]}] : [Handling the incoming request : {dataObject}]")

                addToFile = f"\n[{time.ctime().split(' ')[4][:8]}][Node {self.peerName} received request : {dataObject}]"
                file1 = open(f"peer_{self.peerName}.log", "a")  # append mode
                file1.write(f"{addToFile}\n")
                file1.close()

                #deserialize the incoming request and fulfills it
                if request == 'createTopic':
                    print(f"Route : {dataObject['route']}\n")
                    #if it's final destination, then fulfill the request
                    if len(dataObject['route']) == 1:
                        topic = dataObject['topic']
                        if topic  in self.DHT:
                            response = f"Topic {topic} already exists"
                        else:
                            self.DHT[topic] = []
                            self.subscriptions[topic] = []
                            response = f"Topic {topic} is created!"
                    else:
                        #else find the next node, connect to that node and forward the request
                        route = dataObject['route'][1:]
                        print(f"Next node is Peer {route}")
                        dataObject['route'] = route
                        print(f"Data object to send my neighbor: {dataObject}")
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"

                if request == 'deleteTopic':
                    print(f"Route : {dataObject['route']}\n")
                    #if it's final destination, then fulfill the request
                    if len(dataObject['route']) == 1:
                        topic = dataObject['topic']
                        if topic in self.DHT:
                            del self.DHT[topic]
                            del self.subscriptions[topic]
                            response = f"Topic {topic} has been deleted"
                        else:
                            response = f"Topic {topic} does not exist!"
                    else:
                        #else find the next node, connect to that node and forward the request
                        route = dataObject['route'][1:]
                        print(f"Next node is Peer {route}")
                        dataObject['route'] = route
                        print(f"Data object to send my neighbor: {dataObject}")
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"
                
                if request == 'send':
                    print(f"Route : {dataObject['route']}\n")
                    #if it's final destination, then fulfill the request
                    if len(dataObject['route']) == 1:
                        topic = dataObject['topic']
                        message = dataObject['message']
                        if topic in self.DHT:
                            self.DHT[topic].append(message)
                            response = f"Message {message} has been added to {topic}\n"
                        else:
                            response = f"Topic {topic} does not exist!"
                    else:
                        #else find the next node, connect to that node and forward the request
                        route = dataObject['route'][1:]
                        print(f"Next node is Peer {route}")
                        dataObject['route'] = route
                        print(f"Data object to send my neighbor: {dataObject}")
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"
                
                if request == 'subscribe':
                    print(f"Route : {dataObject['route']}\n")
                    #if it's final destination, then fulfill the request
                    if len(dataObject['route']) == 1:
                        topic = dataObject['topic']
                        caller = dataObject['caller']
                        if topic in self.DHT:
                            if caller in self.subscriptions:
                                response = f"Caller {caller} already subscribed to {topic}\n"
                            else:
                                self.subscriptions[topic].append(caller)
                                self.readHistory[(topic, caller)] = 0
                                response = f"Caller {caller} has subscribed to {topic}\n"
                        else:
                            response = f"Topic {topic} does not exist!"
                    else:
                        #else find the next node, connect to that node and forward the request
                        route = dataObject['route'][1:]
                        print(f"Next node is Peer {route}")
                        dataObject['route'] = route
                        print(f"Data object to send my neighbor: {dataObject}")
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"
                
                if request == 'pull':
                    print(f"Route : {dataObject['route']}\n")
                    #if it's final destination, then fulfill the request
                    if len(dataObject['route']) == 1:
                        topic = dataObject['topic']
                        peerName = dataObject['caller']
                        if topic in self.DHT:
                            if peerName not in self.subscriptions[topic]:
                                response = f"Peer {peerName} is not subscribed to {topic}"
                            else:
                                indexToStart = self.readHistory[(topic, peerName)]
                                if len(self.DHT[topic]) > indexToStart and len(self.DHT[topic]) != 0:
                                    print(self.DHT[topic][indexToStart::])
                                    response = {'response' : self.DHT[topic][indexToStart::]}
                                    print(response)
                                    lastIndexRead = len(self.DHT[topic])
                                    self.readHistory[(topic, peerName)] = lastIndexRead
                                elif len(self.DHT[topic]) == indexToStart or len(self.DHT[topic]) == 0:
                                    response = {'response' : 'Empty'}
                                    print(response)
                                
                                self.garbageCollector(topic)
                        else:
                            response = f"Topic {topic} does not exist to pull"
                    else:
                        #else find the next node, connect to that node and forward the request
                        route = dataObject['route'][1:]
                        print(f"Next node is Peer {route}")
                        dataObject['route'] = route
                        print(f"Data object to send my neighbor: {dataObject}")
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"

                print(f"    [[{time.ctime().split(' ')[4][:8]}]] : [Response back to peer node: {response}]\n")
                response = pickle.dumps(response)
                writer.write(response)
                await writer.drain()

                print(f"\nMessage: {self.DHT}")
                print(f"\nSubscriptions: {self.subscriptions}")

        except Exception as e :
            print(f'\n{e}\n')
            # pass
        
        finally:    
            writer.close()
            await writer.wait_closed()

    async def userInput(self):
        while True:
            command = input("\n2 - CreateTopic\n3 - DeleteTopic\n4 - SendMessage to topic\n5 - Subscribe\n6 - Pull \n7 - exit \nChoose option-->")
            if command == '2':
                topic = input("\nWhat topic you want to create -> ")
                await self.createTopic(topic)
            elif command == '3':
                topic = input("\nWhat topic you want to delete -> ")
                await self.deleteTopic(topic)
            elif command == '4':
                topic = input("\nWhat topic you want to send message -> ")
                message = input("\nWhat message to send -> ")
                print(f"Calling send function\n")
                await self.send(topic, message)
            elif command == '5':
                topic = input("\nWhat topic you want to subscribe -> ")
                await self.subscribe(topic)
            elif command == '6':
                topic = input("\nWhat topic you want to pull from -> ")
                await self.pull(topic)
            if command == "exit" or command == "7":
                print(f"[{time.ctime().split(' ')[4][:8]}] : [Exiting from peer {peerName}]")
                return 
            
            print(f"\n Topics: {self.DHT}")
            print(f"\n Subscriptions: {self.subscriptions}\n")
    
    async def userInputTest(self):
        while True:
            if self.peerID < 4:
                    await asyncio.sleep(1)
                    while True:
                        topic = ''.join(random.choices(string.ascii_letters,k=4))
                        await self.createTopic(topic)
            elif self.peerID > 4 :
                    await asyncio.sleep(1)
                    while True:
                        topic = ''.join(random.choices(string.ascii_letters,k=4))
                        await self.subscribe(topic)
            else:
                return 

    def run_async_function(self, coroutine):
        asyncio.run(coroutine)

    async def run(self):
        try:
            thread_one = threading.Thread(target=self.run_async_function, args=(self.listen_for_messages(),))
            thread_two = threading.Thread(target=self.run_async_function, args=(self.userInput(),))
            thread_one.start()
            thread_two.start()

            thread_one.join()
            thread_two.join()

            print("thread finished")
        except Exception as e:
            print(f"Error occured: {e}")



if __name__ == "__main__":
   if len(sys.argv) > 1:
        peerName = sys.argv[1]
        peer_node = PeerNode(peerName)
        asyncio.run(peer_node.run())


