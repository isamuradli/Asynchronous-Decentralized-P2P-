Welcome

Project : Decentralized Peer-to-Peer publish-subscribe system

This project will support communication of multiple peer nodes. Communication is asyncronous.
Peer nodes can simultaneously publish and subscribe to a topic.

Main file is in nodes/mainNode.py

System support 5 APIs that user can call.
{CreateTopic, DeleteTopic, Send, Subscribe, Pull}

First please install tmux. To do so just do to the main folder of this project and run following:

1. make initial

Peer nodes once connected to the index server, server assigns them a Port number. Peer node will use it for listening incoming
messages from other peers if so.

After instaling tmux, we can go ahead.

1. Open the terminal in the main folder and type tmux
   After that tmux plugin will be open.

2. You can call "make runAll8Peers" which will open 8 session in the background of tmux.
   There will be 8 node from Node A (0) till Node H(7)

3. You can switch to one of the nodes with command Ctrl b + s.

4. After you will see 8 session numerated from session0 to session7

5. You can choose any of them nodes and you will see "Which function do you want to call?"
   There are 5 different functions to call. Last option is 7 or exit.
   You just need to choose from 2 to 7 according to what you want to do.

6. If you choose:

   2. CreateTopic -> terminal will ask topic name
      System itself will calculate the hash value and find the location of the node that should store it. After that if necessary it
      will forward that request to the designated node. If the destination node is not neighbor, it will calculate the path to send it over.
      It will connect to the node that's its neighbor and its neightbor will further forwarding. After the topic reaches the designated Node,
      that node will CREATE the topic and send back the response.

   3. Delete topic -> terminal will ask topic name
      System itself will calculate the hash value and find the location of the node that should store it. After that if necessary it
      will forward that request to the designated node. If the destination node is not neighbor, it will calculate the path to send it over.
      It will connect to the node that's its neighbor and its neightbor will further forwarding. After the topic reaches the designated Node,
      that node will DELETE the topic and send back the response.

   4. Send Message -> terminal will ask topic name and the message content
      System itself will calculate the hash value and find the location of the node that should store it. After that if necessary it
      will forward that request to the designated node. If the destination node is not neighbor, it will calculate the path to send it over.
      It will connect to the node that's its neighbor and its neightbor will further forwarding. After the topic reaches the designated Node,
      that node will check if that topic exist. If so, it will ADD the message to that specified the topic and send back the response.

   5. Subscribe -> terminal will ask topic name
      System itself will calculate the hash value and find the location of the node that should store it. After that if necessary it
      will forward that request to the designated node. If the destination node is not neighbor, it will calculate the path to send it over.
      It will connect to the node that's its neighbor and its neightbor will further forwarding. After the topic reaches the designated Node,
      that node will add the Node into the subscription list of the topic and send back the response.
      Note : you can't subscribe to your own topic

   6. Pull -> will get all unread messages from the topic. Terminal will ask you for the topic name.
      If you have previously read all messages from the topic, you will get Empty.
      Note: you can't pull messages from your topics as you are not subscriber. You are already an owner, what else needed xd

   8. If you want to stop all nodes from running, please run command in main tmux terminal "make killActiveSessions.
      It will close all nodes. P.S Make sure you call that from the main windows session of tmux

When you run peer nodes, we will keep all logs of the operations!!!!

7.  To delete log files, please run: make clean
    Note: Logs are appending mode, if you clean logs from previous session, it will appends on top of data that already in the file

8.  Benchmarking, experiments on hash function and request forwarding:
    If you want to benchmark the APIs and see the number of requests handled by nodes, please do :

    1. Open the tmux in main folder.
    2. Run make benchmark
       It will open 8 new session and run all peer nodes in the background session.
       If you want to see background sessions of peer, press Ctrl b + s
    3. There will be 9 sessions : windows (you are in this session) and session 0 -> sesssion7 ( 8 peer nodes running in the background)
    4. If you want to benchmark specific API, you can open benchmarkAPI.sh and choose the naming from the list that are commented
       from line 19-27
       IMPORTANT NOTE: For benchmarking some APIs such as subscribe, pull, send ( all of these APIs require some topics to exist),
       I first create 25000 topics before benchmarking those APIs.
    5. If you want to stop all running nodes, in main session of tmux run "make killActiveSessions"
