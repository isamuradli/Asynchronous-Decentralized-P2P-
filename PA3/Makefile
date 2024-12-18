runPeer:
	@if [ -z "$(peername)" ]; then \
		echo "No peer Name provided. Please provide it as an argument."; \
		exit 1; \
	fi; \
	echo "Calling..."; \
	python3 nodes/mainNode.py "$(peername)"

install:
	sudo apt update
	sudo apt install -y tmux

benchmark:
	chmod +x benchmarking/benchmarkAPI.sh
	benchmarking/benchmarkAPI.sh

runAll8Peers:
	chmod +x nodes/allNodesUp.sh
	nodes/allNodesUp.sh

killActiveSessions:
	chmod +x nodes/allSessionsClose.sh
	nodes/allSessionsClose.sh
clean:
	rm -f *.log *.txt
	