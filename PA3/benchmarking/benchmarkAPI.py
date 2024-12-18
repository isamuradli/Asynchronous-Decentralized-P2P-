import asyncio, pickle,  threading, time, sys
import socket, hashlib
import random, string
class PeerNode:
    def __init__(self, name, apiName):
        self.peerName = name
        self.apiName = apiName
        self.peerID = int(ord(self.peerName)-65)
        self.peerPort = 5000+int(ord(self.peerName))-65
        self.DHT = {}
        self.subscriptions = {}
        self.readHistory = {}
        self.grbCollectIndex = 0
        self.averageResponseForwarding = 0
        self.lock = asyncio.Lock()
        self.distributionList = { 0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0 }

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
                file.write(f"\n[{time.ctime().split(' ')[3][:8]}] : {message}")

    async def hashTheTopic(self, topic):    
        hash_object = hashlib.sha256(topic.encode())
        hash_int = int(hash_object.hexdigest(), 16)
        node_index = hash_int % 8
        return node_index
    
    async def checkDistributionOfHash(self):
        for _ in range(1000000):
            topic = ''.join(random.choices(string.ascii_letters,k=5))
            nodeIdx = await self.hashTheTopic(topic)
            self.distributionList[nodeIdx] += 1
        
        print(f"Distribution of 1 million topics: \n")
        for i, j in self.distributionList.items():
            print(f"Node {i} has been assigned {j} number of topics")
              
    async def measureAverageTime(self):
        total = 0
        for _ in range(999999):
            startTime = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            await self.hashTheTopic(topic)
            endTime = time.time()
            total += endTime - startTime
        
        averageTime = total / 999999
        return averageTime

    async def findHypercubeRoute(self, source, destination):
        callingNode = format(source, 'b').zfill(3)  
        targetNode = format(destination, 'b').zfill(3)
        
        print(f"Source: {callingNode}, Destination: {targetNode}")
        
        # Store the route
        route = []
        
        current = source
        while current != destination:
            route.append(current)
            
           
            for i in range(len(callingNode)):
                if (current >> i) & 1 != (destination >> i) & 1:
                    current ^= (1 << i)  
                    break
        
        route.append(destination)  
        return route

    async def checkIfNeighbor(self, node):
        print(f"Checking if node is peer.\n")
        print(f"SelfID : {self.peerID}, peerToConnect : {int(node)}\n")
        
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
            route = await self.findHypercubeRoute(self.peerID, peerToWrite)
            route = route[1:]
            nextNodeInTransit = route[0]
            startTime = time.time()
            reader, writer = await asyncio.open_connection('127.0.1.1', 5000+nextNodeInTransit)
            request = {
                'requestName': 'createTopic', 
                'caller': self.peerName, 
                'peerToWrite' : peerToWrite,
                'route' : route,
                'topic': topic
                }
            
            addToFile = f"[Node {self.peerName} called : {request}]"
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            # 

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)
            endTime = time.time()

            self.averageResponseForwarding += (endTime-startTime)

            addToFile = f"[Response from Peer Node {peerToWrite} : {response}]"
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            
            writer.close()
            await writer.wait_closed()

    async def deleteTopic(self, topic):
        print("\nDeleting topic...")

        peerToDelete = await self.hashTheTopic(topic)

        #check if topic has been assinged to node itself.
        if peerToDelete == int(self.peerID):
            if topic in self.DHT:
                print(f"Topic {topic} already delete")
                del self.DHT[topic]
                del self.subscriptions[topic]
            else:
                print(f"Topic does not exist\n")
            return
        else:
            route = await self.findHypercubeRoute(self.peerID, peerToDelete)
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
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            # 

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)

            addToFile = f"[Response: {response}]"
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            
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
            route = await self.findHypercubeRoute(self.peerID, peerToWrite)
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
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)

            addToFile = f"[Response: {response}]"
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            

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
            route = await self.findHypercubeRoute(self.peerID, peerToWrite)
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
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)

            addToFile = f"[Response: {response}]"
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            

            writer.close()
            await writer.wait_closed()
      
    async def pull(self, topic):

        #Check which Node to write
        peerToWrite = await self.hashTheTopic(topic)
        
        #check if topic has been assinged to node itself.
        if peerToWrite == int(self.peerID):
            print(f"You can't pull from your own topic!\n")
        else:
            route = await self.findHypercubeRoute(self.peerID, peerToWrite)
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
            
            # addToFile = f"[Node {self.peerName} called : {request}]"
            # print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            

            message = pickle.dumps(request)
            writer.write(message)

            await writer.drain()
            
            data = await reader.read(200)
            response = pickle.loads(data)

            addToFile = f"[Response: {response}]"
            print(f"\n[{time.ctime().split(' ')[3][:8]}] : {addToFile}")
            

            writer.close()
            await writer.wait_closed()

    async def send_messageToPeer(self, peerIP, peerPort, message):
        reader, writer = await asyncio.open_connection(peerIP, peerPort)
        
        request = pickle.dumps(message)
        writer.write(request)
        await writer.drain()

        data = await reader.read(2000)
        response = pickle.loads(data)
        print(f"\n[{time.ctime().split(' ')[3][:8]}] : [Node {self.peerName} received reply: {response}]")
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
            # print(f"\n[{time.ctime().split(' ')[3][:8]}] : [Node {self.peerName} accepted connection from {addr}]")

            while True:
                data = await reader.read(200)
                if not data:
                    break
                dataObject = pickle.loads(data)
                request = dataObject['requestName']
                # print(f"[{time.ctime().split(' ')[3][:8]}] : [Handling the incoming request : {dataObject}]")

                if request == 'createTopic':
                    if len(dataObject['route']) == 1:
                        topic = dataObject['topic']
                        if topic  in self.DHT:
                            response = f"Topic {topic} already exists"
                        else:
                            self.DHT[topic] = []
                            self.subscriptions[topic] = []
                            response = f"Topic {topic} is created!"
                    else:
                        route = dataObject['route'][1:]
                        dataObject['route'] = route
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"

                if request == 'deleteTopic':
                    if len(dataObject['route']) == 1:
                        topic = dataObject['topic']
                        if topic in self.DHT:
                            del self.DHT[topic]
                            del self.subscriptions[topic]
                            response = f"Topic {topic} has been deleted"
                        else:
                            response = f"Topic {topic} does not exist!"
                    else:
                        route = dataObject['route'][1:]
                        dataObject['route'] = route
                        print(f"Data object to send my neighbor: {dataObject}")
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"
                
                if request == 'send':
                    if len(dataObject['route']) == 1:
                        topic = dataObject['topic']
                        message = dataObject['message']
                        if topic in self.DHT:
                            self.DHT[topic].append(message)
                            response = f"Message {message} has been added to {topic}\n"
                        else:
                            response = f"Topic {topic} does not exist!"
                    else:
                        route = dataObject['route'][1:]
                        dataObject['route'] = route
                        print(f"Data object to send my neighbor: {dataObject}")
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"
                
                if request == 'subscribe':
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
                        route = dataObject['route'][1:]
                        dataObject['route'] = route
                        print(f"Data object to send my neighbor: {dataObject}")
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"
                
                if request == 'pull':
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
                                    lastIndexRead = len(self.DHT[topic])
                                    self.readHistory[(topic, peerName)] = lastIndexRead
                                    
                                elif len(self.DHT[topic]) == indexToStart or len(self.DHT[topic]) == 0:
                                    response = {'response' : 'Empty'}
                                    print(response)
                                
                                self.garbageCollector(topic)
                        else:
                            response = f"Topic {topic} does not exist to pull"
                            
                    else:
                        route = dataObject['route'][1:]
                        dataObject['route'] = route
                        # print(f"Data object to send my neighbor: {dataObject}")
                        response = await self.send_messageToPeer('127.0.1.1', 5000+route[0], dataObject)
                        response = f"{response}"

                # print(f"    [[{time.ctime().split(' ')[3][:8]}]] : [Response back to peer node: {response}]\n")
                response = pickle.dumps(response)
                writer.write(response)
                await writer.drain()

                # print(f"\nMessage: {self.DHT}")
                # print(f"\nSubscriptions: {self.subscriptions}")

        # except Exception as e :
        #     print(f'\n{e}\n')
        #     # pass
        
        finally:    
            writer.close()
            await writer.wait_closed()

    async def benchmarkCreateTopic(self):
        await asyncio.sleep(0.3)
        startTime = time.time()
        numOfRequests = 0
        latency = 0
        while True:
                requestSend = time.time()
                topic = ''.join(random.choices(string.ascii_letters,k=4))
                timePassed = time.time() - startTime
                if timePassed >= 300:
                    break
                numOfRequests += 1
                print(numOfRequests)
                await self.createTopic(topic)
                requestReceived = time.time()
                latency += (requestReceived-requestSend)

        print(f"\n\nBenchmarking statistics for {self.apiName}: \nNode {self.peerName}:\t\n Number of request : {numOfRequests}, \t\ntimePassed: 5 mins, \t\nthroughput : {numOfRequests/300} \t\nLatency = {latency/numOfRequests}" )
    
    async def benchmarkDeleteTopic(self):
        await asyncio.sleep(0.3)
        numOfRequests = 0
        latency = 0
        for x in range(25000):
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            print(x)
            await self.createTopic(topic)
        startTime = time.time()
        while True:
            requestSend = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            timePassed = time.time() - startTime
            if timePassed >= 300:
                break
            numOfRequests += 1
            await self.deleteTopic(topic)
            requestReceived = time.time()
            latency += (requestReceived-requestSend)
        
        print(f"\n\nBenchmarking statistics for {self.apiName}: \nNode {self.peerName}:\t\n Number of request : {numOfRequests}, \t\ntimePassed: 5 mins, \t\nthroughput : {numOfRequests/300} \t\nLatency = {latency/numOfRequests}" )

    async def benchmarkSend(self):
        await asyncio.sleep(0.3)
        numOfRequests = 0
        latency = 0
        for x in range(25000):
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            await self.createTopic(topic)
        startTime = time.time()
        while True:
            requestSend = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            msg = ''.join(random.choices(string.ascii_letters,k=4))
            timePassed = time.time() - startTime
            if timePassed >= 300:
                break
            numOfRequests += 1
            await self.send(topic, msg)
            requestReceived = time.time()
            latency += (requestReceived-requestSend)
        print(f"\n\nBenchmarking statistics for {self.apiName}: \nNode {self.peerName}:\t\n Number of request : {numOfRequests}, \t\ntimePassed: 5 mins, \t\nthroughput : {numOfRequests/300} \t\nLatency = {latency/numOfRequests}" )
        
    async def benchmarkSubscribe(self):
        await asyncio.sleep(0.3)
        numOfRequests = 0
        latency = 0
        for x in range(25000):
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            print(x)
            await self.createTopic(topic)
        startTime = time.time()
        while True:
            requestSend = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            timePassed = time.time() - startTime
            if timePassed >= 300:
                break
            numOfRequests += 1
            await self.subscribe(topic)
            requestReceived = time.time()
            latency += (requestReceived-requestSend)
        print(f"\n\nBenchmarking statistics for {self.apiName}: \nNode {self.peerName}:\t\n Number of request : {numOfRequests}, \t\ntimePassed: 5 mins, \t\nthroughput : {numOfRequests/300} \t\nLatency = {latency/numOfRequests}" )

    async def benchmarkPull(self):
        await asyncio.sleep(1)
        numOfRequests = 0
        latency = 0
        for x in range(25000):
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            msg = ''.join(random.choices(string.ascii_letters,k=4))
            print(x)
            await self.createTopic(topic)
            await self.send(topic, msg)
            await self.subscribe(topic)

        startTime = time.time()
        while True:
            requestSend = time.time()
            topic = ''.join(random.choices(string.ascii_letters,k=4))
            timePassed = time.time() - startTime
            if timePassed >= 300:
                break
            numOfRequests += 1
            await self.pull(topic)
            requestReceived = time.time()
            latency += (requestReceived-requestSend)
        print(f"\n\nBenchmarking statistics for {self.apiName}: \nNode {self.peerName}:\t\n Number of request : {numOfRequests}, \t\ntimePassed: 5 mins, \t\nthroughput : {numOfRequests/300} \t\nLatency = {latency/numOfRequests}" )

    async def userInput(self, apiName):
        print(f"\nStarting benchmarking {apiName}\n")
        if apiName =="createTopic":
            await self.benchmarkCreateTopic()
        if apiName =="deleteTopic":
            await self.benchmarkDeleteTopic()
        if apiName =="send":
            await self.benchmarkSend()
        if apiName =="subscribe":
            await self.benchmarkSubscribe()
        if apiName =="pull":
            await self.benchmarkPull()
        if apiName =="measureAverageTime":
            res = await self.measureAverageTime()
            print(f"Average time cost for Peer {self.peerName} -> \n{res}")
        if apiName =="distribute":
            await self.checkDistributionOfHash()
        if apiName =="forwarding":
            await self.benchmarkForwarding()

    async def benchmarkForwarding(self):
        await asyncio.sleep(0.5)
        startTime = time.time()
        numOfRequests = 0
        while True:
                topic = ''.join(random.choices(string.ascii_letters,k=4))
                timePassed = time.time() - startTime
                if timePassed >= 300:
                    break
                numOfRequests += 1
                await self.createTopic(topic)

        print(f"\n\nBenchmarking statistics for forwarding for 5 mins: \nAverage Response time: {self.averageResponseForwarding/numOfRequests} \nThroughput : {numOfRequests/300}" )
    

    def run_async_function(self, coroutine):
        asyncio.run(coroutine)


    async def run(self):
        try:
            thread_one = threading.Thread(target=self.run_async_function, args=(self.listen_for_messages(),))
            thread_two = threading.Thread(target=self.run_async_function, args=(self.userInput(self.apiName),))
            thread_one.start()
            thread_two.start()

            thread_one.join()
            thread_two.join()

            print("thread finished")
        except Exception as e:
            print(f"Error occured: {e}")



if __name__ == "__main__":
   if len(sys.argv) > 2:
        peerName = sys.argv[1]
        apiName = sys.argv[2]
        peer_node = PeerNode(peerName, apiName)
        asyncio.run(peer_node.run())