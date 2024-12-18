tmux new-session -d -s session0
tmux new-session -d -s session1
tmux new-session -d -s session2
tmux new-session -d -s session3
tmux new-session -d -s session4
tmux new-session -d -s session5
tmux new-session -d -s session6
tmux new-session -d -s session7



tmux send-keys -t session0:0 "make runPeer peername=A " C-m
tmux send-keys -t session1:0 "make runPeer peername=B " C-m
tmux send-keys -t session2:0 "make runPeer peername=C " C-m
tmux send-keys -t session3:0 "make runPeer peername=D " C-m
tmux send-keys -t session4:0 "make runPeer peername=E " C-m
tmux send-keys -t session5:0 "make runPeer peername=F " C-m
tmux send-keys -t session6:0 "make runPeer peername=G " C-m
tmux send-keys -t session7:0 "make runPeer peername=H " C-m