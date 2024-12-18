tmux new-session -d -s session0
tmux new-session -d -s session1
tmux new-session -d -s session2
tmux new-session -d -s session3
tmux new-session -d -s session4
tmux new-session -d -s session5
tmux new-session -d -s session6
tmux new-session -d -s session7

tmux send-keys -t session0:0 "python3 benchmarking/benchmarkAPI.py A createTopic" C-m
tmux send-keys -t session1:0 "python3 benchmarking/benchmarkAPI.py B createTopic" C-m
tmux send-keys -t session2:0 "python3 benchmarking/benchmarkAPI.py C createTopic" C-m
tmux send-keys -t session3:0 "python3 benchmarking/benchmarkAPI.py D createTopic" C-m
tmux send-keys -t session4:0 "python3 benchmarking/benchmarkAPI.py E createTopic" C-m
tmux send-keys -t session5:0 "python3 benchmarking/benchmarkAPI.py F createTopic" C-m
tmux send-keys -t session6:0 "python3 benchmarking/benchmarkAPI.py G createTopic" C-m
tmux send-keys -t session7:0 "python3 benchmarking/benchmarkAPI.py H createTopic" C-m

#Please choose on of the APIs
#createTopic
#deleteTopic
#send
#subscribe
#pull
#measureAverageTime
#distribute
#forwarding