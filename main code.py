import random
import time
from enum import Enum
import threading
from queue import Queue, Empty

# --- Configuration ---
CLUSTER_SIZE = 5
HEARTBEAT_INTERVAL = 0.1  # Leader sends heartbeats every 100ms
ELECTION_TIMEOUT_MIN = 0.3  # Min election timeout (300ms)
ELECTION_TIMEOUT_MAX = 0.6  # Max election timeout (600ms)
NETWORK_LATENCY = 0.05      # Simulated network delay

class State(Enum):
    """The three possible roles of a Raft server."""
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

# --- RPC Message Structures ---
class RequestVoteArgs:
    def __init__(self, term, candidateId, lastLogIndex, lastLogTerm):
        self.term = term
        self.candidateId = candidateId
        self.lastLogIndex = lastLogIndex
        self.lastLogTerm = lastLogTerm

class RequestVoteReply:
    def __init__(self, term, voteGranted):
        self.term = term
        self.voteGranted = voteGranted

class AppendEntriesArgs:
    def __init__(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        # 'entries' will be empty for heartbeats in this simplified version
        self.term = term
        self.leaderId = leaderId
        self.prevLogIndex = prevLogIndex
        self.prevLogTerm = prevLogTerm
        self.entries = entries
        self.leaderCommit = leaderCommit

class AppendEntriesReply:
    def __init__(self, term, success):
        self.term = term
        self.success = success

# --- The Raft Server Class ---
class RaftNode:
    def __init__(self, id, cluster, network_queue):
        self.id = id
        self.cluster = cluster        # List of all node IDs
        self.network_queue = network_queue # Global queue for message passing

        # --- Persistent State (must be saved to disk in a full implementation) ---
        self.current_term = 0
        self.voted_for = None
        self.log = [(0, 0)] # (term, command) - sentinel entry: (term=0, index=0)

        # --- Volatile State ---
        self.state = State.FOLLOWER
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        
        # --- Timers and Concurrency ---
        self.timeout_thread = None
        self.stop_event = threading.Event()
        self.last_heartbeat_time = time.time()
        self.lock = threading.Lock()
        
    def _get_random_timeout(self):
        """Randomized election timeout to prevent split votes."""
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    # --- Core State Transitions ---
    
    def become_follower(self, new_term):
        with self.lock:
            if new_term > self.current_term:
                self.current_term = new_term
                self.voted_for = None # Reset vote
                
            self.state = State.FOLLOWER
            self.leader_id = None
            self.reset_election_timer()
            print(f"Node {self.id}: -> FOLLOWER for Term {self.current_term}")

    def become_candidate(self):
        with self.lock:
            self.state = State.CANDIDATE
            self.current_term += 1
            self.voted_for = self.id
            self.votes_received = 1 # Vote for self
            self.leader_id = None
            
            print(f"Node {self.id}: -> CANDIDATE for Term {self.current_term}")
            
            self.reset_election_timer() # Reset and start new timer for this term
            
            # Send RequestVote RPCs to all other servers
            for peer_id in self.cluster:
                if peer_id != self.id:
                    self._send_request_vote(peer_id)

    def become_leader(self):
        with self.lock:
            self.state = State.LEADER
            self.leader_id = self.id
            
            # Re-initialize leader volatile state
            self.next_index = {nid: len(self.log) + 1 for nid in self.cluster if nid != self.id}
            self.match_index = {nid: 0 for nid in self.cluster if nid != self.id}

            print(f"\n*** Node {self.id}: ELECTED LEADER for Term {self.current_term} ***\n")
            
            # Immediately send first heartbeats
            self._send_heartbeats()
            self.last_heartbeat_time = time.time()
            self.reset_election_timer() # Start heartbeat timer

    # --- RPC Senders ---
    
    def _send_request_vote(self, target_id):
        # Note: log index and term are simplified here
        args = RequestVoteArgs(self.current_term, self.id, len(self.log) - 1, self.log[-1][0])
        # Simulate network delay by putting message in queue
        self.network_queue.put(('RequestVote', self.id, target_id, args, time.time()))

    def _send_heartbeats(self):
        args = AppendEntriesArgs(self.current_term, self.id, len(self.log) - 1, self.log[-1][0], [], self.commit_index)
        for peer_id in self.cluster:
            if peer_id != self.id:
                self.network_queue.put(('AppendEntries', self.id, peer_id, args, time.time()))

    # --- RPC Handlers (Receiver Logic) ---

    def handle_request_vote(self, sender_id, args):
        with self.lock:
            reply = RequestVoteReply(self.current_term, False)
            
            # Rule 1: All Servers, If RPC term > currentTerm, set currentTerm = RPC term and become Follower
            if args.term > self.current_term:
                self.become_follower(args.term)
            
            if args.term < self.current_term:
                # Reply false if term < currentTerm
                pass 
            elif self.voted_for is None or self.voted_for == args.candidateId:
                # If votedFor is null or candidateId, then grant vote if candidate's log is up-to-date
                # Simplified Log Up-to-Date check (True means log is sufficient)
                if self._is_log_up_to_date(args.lastLogIndex, args.lastLogTerm):
                    reply.voteGranted = True
                    self.voted_for = args.candidateId
                    self.reset_election_timer()
            
            # Send reply back to the network queue
            self.network_queue.put(('RequestVoteReply', self.id, sender_id, reply, time.time()))
            
    def handle_append_entries(self, sender_id, args):
        with self.lock:
            reply = AppendEntriesReply(self.current_term, False)
            
            # Rule 1: All Servers, If RPC term > currentTerm, set currentTerm = RPC term and become Follower
            if args.term > self.current_term:
                self.become_follower(args.term)
                
            if args.term < self.current_term:
                # Reply false if term < currentTerm
                pass
            else:
                # Valid Leader found, reset timer and confirm state (must be Follower now)
                self.leader_id = args.leaderId
                if self.state != State.FOLLOWER:
                    self.become_follower(args.term)
                self.reset_election_timer()
                reply.success = True
                
            # Send reply back
            self.network_queue.put(('AppendEntriesReply', self.id, sender_id, reply, time.time()))

    # --- Vote Counting Logic ---
    
    def handle_request_vote_reply(self, sender_id, reply):
        with self.lock:
            # Rule 1: All Servers, If RPC term > currentTerm, set currentTerm = RPC term and become Follower
            if reply.term > self.current_term:
                self.become_follower(reply.term)
                return

            if self.state == State.CANDIDATE and reply.voteGranted and reply.term == self.current_term:
                self.votes_received += 1
                
                # Check for majority
                majority = CLUSTER_SIZE // 2 + 1
                if self.votes_received >= majority:
                    self.become_leader()
                    
    # --- Dummy Log Check (Needs full implementation) ---
    def _is_log_up_to_date(self, lastLogIndex, lastLogTerm):
        """
        Simplified Log Up-to-Date Rule check.
        """
        voter_last_term = self.log[-1][0]
        voter_last_index = len(self.log) - 1
        
        # If candidate's log is more current (higher term or same term, longer log), grant vote.
        if lastLogTerm > voter_last_term:
            return True
        if lastLogTerm == voter_last_term and lastLogIndex >= voter_last_index:
            return True
        return False
        
    # --- Timer/Main Loop ---

    def run(self):
        """Main thread loop for receiving and processing messages."""
        
        while not self.stop_event.is_set():
            # 1. Handle incoming messages from the network queue
            try:
                # Non-blocking check for messages with a timeout
                message_type, sender_id, target_id, data, send_time = self.network_queue.get(timeout=0.01) 
                
                # Simulate network latency (only process if latency period has passed)
                if time.time() - send_time < NETWORK_LATENCY:
                    # Re-queue the message to be processed later
                    self.network_queue.put((message_type, sender_id, target_id, data, send_time))
                    continue
                
                if target_id != self.id:
                    # Message is not for this node, return it to the queue immediately
                    self.network_queue.put((message_type, sender_id, target_id, data, send_time)) 
                    continue

                # Process the message
                if message_type == 'RequestVote':
                    self.handle_request_vote(sender_id, data)
                elif message_type == 'AppendEntries':
                    self.handle_append_entries(sender_id, data)
                elif message_type == 'RequestVoteReply':
                    self.handle_request_vote_reply(sender_id, data)
                
            except Empty: # queue.Empty is the typical exception when timeout occurs
                pass

            # 2. Leader specific periodic actions (Heartbeats)
            if self.state == State.LEADER:
                if time.time() - self.last_heartbeat_time >= HEARTBEAT_INTERVAL:
                    self._send_heartbeats()
                    self.last_heartbeat_time = time.time()

            # 3. Small sleep to avoid hogging CPU
            time.sleep(0.005) 

    def reset_election_timer(self):
        """Stops the old timer and starts a new one with a fresh timeout."""
        if self.timeout_thread and self.timeout_thread.is_alive():
            self.timeout_thread.cancel()
            
        # Set a new, randomized timeout
        election_timeout = self._get_random_timeout()
        
        self.timeout_thread = threading.Timer(election_timeout, self._election_timeout_handler)
        self.timeout_thread.daemon = True
        self.timeout_thread.start()
        
    def _election_timeout_handler(self):
        """Called when the election timer expires."""
        with self.lock:
            # If the timer expires and the node is not the Leader, it must start an election.
            if self.state != State.LEADER:
                print(f"Node {self.id}: Election Timeout! Starting a new election.")
                self.become_candidate()

# --- Cluster Simulation ---
if __name__ == "__main__":
    node_ids = list(range(1, CLUSTER_SIZE + 1))
    # Centralized queue to simulate network communication. 
    # Messages contain: (type, sender_id, target_id, data, send_time)
    network_queue = Queue() 
    nodes = {}
    
    # 1. Initialize all nodes
    for nid in node_ids:
        nodes[nid] = RaftNode(nid, node_ids, network_queue)

    threads = []
    
    # 2. Start all node threads
    print("Starting Raft Cluster Simulation (Leader Election Only)")
    for node in nodes.values():
        t = threading.Thread(target=node.run)
        t.daemon = True
        t.start()
        threads.append(t)

    # 3. Run simulation for a set time
    try:
        SIMULATION_TIME = 7
        print(f"Running simulation for {SIMULATION_TIME} seconds...")
        time.sleep(SIMULATION_TIME) 
        
        # Final check
        leader_count = sum(1 for node in nodes.values() if node.state == State.LEADER)
        leader_id = next((node.id for node in nodes.values() if node.state == State.LEADER), "None")
        
        print("\n--- Simulation Complete ---")
        print(f"Final Leader Count: {leader_count}")
        print(f"Final Leader ID: {leader_id}")
        
    except KeyboardInterrupt:
        print("\nSimulation interrupted.")
        
    # 4. Cleanup and stop threads
    for node in nodes.values():
        node.stop_event.set()
        if node.timeout_thread:
            node.timeout_thread.cancel()

    print("Raft simulation ended.")
