import random
import time
from threading import Thread, Lock

# --- Configuration ---
MIN_ELECTION_TIMEOUT = 0.150  # seconds
MAX_ELECTION_TIMEOUT = 0.300  # seconds
HEARTBEAT_INTERVAL = 0.050    # Must be less than MIN_ELECTION_TIMEOUT

# --- Server States ---
class ServerState:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2

# --- Raft Server Class ---
class RaftServer:
    def __init__(self, server_id, all_server_ids, message_bus):
        self.server_id = server_id
        self.all_server_ids = all_server_ids
        self.message_bus = message_bus
        self.lock = Lock()
        
        # Persistent State
        self.current_term = 0
        self.voted_for = None
        self.log = [] # List of (term, command) tuples

        # Volatile State
        self.state = ServerState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.last_log_index = -1
        
        # Election/Timing
        self.votes_received = set()
        self.next_election_timeout = self._generate_election_timeout()
        self.last_heartbeat_time = time.time()
        
        self.is_running = True
        self.thread = Thread(target=self._run_loop)
        
        print(f"Server {self.server_id} initialized as FOLLOWER in Term 0.")

    def _generate_election_timeout(self):
        """Generates a randomized election timeout for concurrency safety."""
        return random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)

    def _transition_to_follower(self, new_term):
        with self.lock:
            if new_term > self.current_term:
                self.current_term = new_term
                self.voted_for = None
            
            # Reset timer and state
            self.state = ServerState.FOLLOWER
            self.last_heartbeat_time = time.time()
            self.next_election_timeout = self._generate_election_timeout()
            print(f"Server {self.server_id} transitioned to **FOLLOWER** in Term {self.current_term}.")

    def _transition_to_candidate(self):
        with self.lock:
            self.state = ServerState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.server_id # Vote for self
            self.votes_received = {self.server_id}
            self.last_heartbeat_time = time.time() # Reset timer
            self.next_election_timeout = self._generate_election_timeout()
            
            print(f"Server {self.server_id} transitioned to **CANDIDATE** in Term {self.current_term}. Starting election.")
            
            # Initiate RequestVote RPCs
            self._send_request_vote()

    def _transition_to_leader(self):
        with self.lock:
            if self.state == ServerState.LEADER:
                return
                
            self.state = ServerState.LEADER
            print(f"Server {self.server_id} is now **LEADER** in Term {self.current_term}! 🎉")
            
            # Leader Volatile State Initialization
            self.next_index = {sid: self.last_log_index + 1 for sid in self.all_server_ids if sid != self.server_id}
            self.match_index = {sid: -1 for sid in self.all_server_ids if sid != self.server_id}
            
            # Send initial empty AppendEntries (heartbeat) to establish authority
            self._send_heartbeats()

    # --- RPC Senders (Simulated) ---

    def _send_request_vote(self):
        """Sends RequestVote RPCs to all other servers."""
        for target_id in self.all_server_ids:
            if target_id != self.server_id:
                # Simulate RPC call via message bus
                self.message_bus.send_message(
                    target_id,
                    'RequestVote',
                    {
                        'term': self.current_term,
                        'candidate_id': self.server_id,
                        'last_log_index': self.last_log_index,
                        'last_log_term': self.log[self.last_log_index][0] if self.log else 0
                    }
                )

    def _send_heartbeats(self):
        """Sends empty AppendEntries RPCs to all followers."""
        with self.lock:
            self.last_heartbeat_time = time.time()
            
            for target_id in self.all_server_ids:
                if target_id != self.server_id:
                    # Leader uses its own state to send the AppendEntries
                    next_idx = self.next_index.get(target_id, 0)
                    prev_log_index = next_idx - 1
                    prev_log_term = self.log[prev_log_index][0] if prev_log_index >= 0 else 0
                    
                    self.message_bus.send_message(
                        target_id,
                        'AppendEntries',
                        {
                            'term': self.current_term,
                            'leader_id': self.server_id,
                            'prev_log_index': prev_log_index,
                            'prev_log_term': prev_log_term,
                            'entries': [], # Empty for heartbeat
                            'leader_commit': self.commit_index
                        }
                    )
                    
    # --- RPC Receivers/Handlers ---

    def handle_rpc(self, type, payload):
        """Main entry point for receiving simulated RPCs."""
        with self.lock:
            if payload['term'] > self.current_term:
                self._transition_to_follower(payload['term'])

        if type == 'RequestVote':
            return self._handle_request_vote(payload)
        elif type == 'RequestVoteResponse':
            return self._handle_request_vote_response(payload)
        elif type == 'AppendEntries':
            return self._handle_append_entries(payload)
        elif type == 'AppendEntriesResponse':
            # This logic is crucial for log replication (Task 3) but simplified here
            pass 

    def _handle_request_vote(self, payload):
        """Implements the core RequestVote logic (Task 1)."""
        term = payload['term']
        candidate_id = payload['candidate_id']
        
        vote_granted = False
        
        with self.lock:
            # 1. Reject if candidate's term is smaller
            if term < self.current_term:
                pass
            # 2. Grant vote if server hasn't voted OR already voted for this candidate,
            #    AND the candidate's log is up-to-date.
            elif self.voted_for in [None, candidate_id]:
                # Log Safety Check (simplified: always vote if terms match, ignoring length)
                # Proper Raft requires checking lastLogIndex and lastLogTerm
                vote_granted = True
                self.voted_for = candidate_id
                self.last_heartbeat_time = time.time() # Reset timer upon granting vote
        
        # Send back the response
        self.message_bus.send_message(
            candidate_id, 
            'RequestVoteResponse', 
            {'term': self.current_term, 'vote_granted': vote_granted, 'voter_id': self.server_id}
        )

    def _handle_request_vote_response(self, payload):
        """Candidate collects and tallies votes."""
        with self.lock:
            if self.state != ServerState.CANDIDATE or payload['term'] < self.current_term:
                return

            if payload['vote_granted']:
                self.votes_received.add(payload['voter_id'])
                
            # Check for majority
            majority = len(self.all_server_ids) // 2 + 1
            if len(self.votes_received) >= majority:
                self._transition_to_leader()

    def _handle_append_entries(self, payload):
        """Handles heartbeats and log replication (Task 1 & 3)."""
        term = payload['term']
        
        with self.lock:
            if term < self.current_term:
                success = False
            else:
                # If leader's term is current or greater, transition to Follower 
                # (even if already a Follower, this resets the election timer)
                self._transition_to_follower(term)
                success = True
                # Log replication logic (Task 3) would go here
            
            # Send back the response
            self.message_bus.send_message(
                payload['leader_id'],
                'AppendEntriesResponse',
                {'term': self.current_term, 'success': success, 'follower_id': self.server_id}
            )

    # --- Main Loop and Timing (Task 2: Reliable Timeout) ---

    def _run_loop(self):
        """The main thread loop for timing and state checks."""
        while self.is_running:
            time.sleep(HEARTBEAT_INTERVAL) # Small sleep to avoid busy waiting
            
            with self.lock:
                # 1. Leader Logic: Send Heartbeats
                if self.state == ServerState.LEADER:
                    if time.time() - self.last_heartbeat_time >= HEARTBEAT_INTERVAL:
                        self._send_heartbeats()
                
                # 2. Follower/Candidate Logic: Check Election Timeout
                elif self.state in [ServerState.FOLLOWER, ServerState.CANDIDATE]:
                    elapsed = time.time() - self.last_heartbeat_time
                    if elapsed >= self.next_election_timeout:
                        # Timeout detected -> start new election
                        self._transition_to_candidate()
                        
    def start(self):
        self.thread.start()

    def stop(self):
        self.is_running = False
        self.thread.join()
        
# --- Simulation Environment: Message Bus ---
class MessageBus:
    def __init__(self, servers):
        self.servers = servers
        self.queue = []
        self.lock = Lock()
        
    def send_message(self, target_id, type, payload):
        with self.lock:
            # Simulate a slight network delay
            time.sleep(random.uniform(0.001, 0.010)) 
            self.queue.append({'target': target_id, 'type': type, 'payload': payload})

    def process_messages(self):
        """Pulls messages from the queue and dispatches them to servers."""
        while True:
            time.sleep(0.005)
            
            with self.lock:
                if not self.queue:
                    continue
                
                message = self.queue.pop(0)
                
            target_server = self.servers.get(message['target'])
            if target_server:
                # Dispatch the message to the target server's handler
                target_server.handle_rpc(message['type'], message['payload'])

# --- Main Simulation Execution ---
if __name__ == "__main__":
    SERVER_COUNT = 5
    SERVER_IDS = list(range(1, SERVER_COUNT + 1))

    # Initialize components
    all_servers = {}
    message_bus = MessageBus(all_servers)

    # Create and link servers
    for sid in SERVER_IDS:
        all_servers[sid] = RaftServer(sid, SERVER_IDS, message_bus)
    
    # Start all server threads
    for server in all_servers.values():
        server.start()
        
    # Start the message bus processing thread
    bus_thread = Thread(target=message_bus.process_messages)
    bus_thread.daemon = True
    bus_thread.start()

    print("\n--- Simulation Started (5 Nodes) ---\n")
    print(f"Election timeouts range: {MIN_ELECTION_TIMEOUT}s - {MAX_ELECTION_TIMEOUT}s.")
    print(f"Heartbeat interval: {HEARTBEAT_INTERVAL}s.\n")

    # Run the simulation for a period
    SIMULATION_TIME = 5 # seconds
    time.sleep(SIMULATION_TIME) 

    # Clean up and report final state
    for server in all_servers.values():
        server.stop()
        
    final_leader = [s.server_id for s in all_servers.values() if s.state == ServerState.LEADER]
    print("\n--- Simulation Ended ---")
    if final_leader:
        print(f"Final Leader: Server {final_leader[0]} (Term {all_servers[final_leader[0]].current_term})")
    else:
        print("No leader elected or election is ongoing.")
