import threading
import time
import random

# ------------------------------------------------------
# ------------------   RAFT NODE   ---------------------
# ------------------------------------------------------

class RaftNode(threading.Thread):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"

    def __init__(self, node_id, cluster):
        super().__init__()
        self.node_id = node_id
        self.cluster = cluster

        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0

        # Leader state (only for leaders)
        self.next_index = {}
        self.match_index = {}

        # Node state
        self.state = RaftNode.FOLLOWER
        self.votes_received = 0
        self.running = True

        # Timeouts
        self.reset_election_timeout()

    def reset_election_timeout(self):
        self.election_timeout = time.time() + random.uniform(2, 4)

    # ------------------------------------------------------
    # ------------------ MESSAGE HANDLERS ------------------
    # ------------------------------------------------------

    def request_vote(self, term, candidate_id):
        """Handle RequestVote RPC."""
        if term < self.current_term:
            return False, self.current_term

        if term > self.current_term:
            self.current_term = term
            self.state = RaftNode.FOLLOWER
            self.voted_for = None

        if self.voted_for is None:
            self.voted_for = candidate_id
            self.reset_election_timeout()
            print(f"[Node {self.node_id}] Votes for Node {candidate_id} (Term {term})")
            return True, self.current_term

        return False, self.current_term

    def append_entries(self, term, leader_id, entries=None):
        """Heartbeat + log replication."""
        if term < self.current_term:
            return False

        self.state = RaftNode.FOLLOWER
        self.current_term = term
        self.reset_election_timeout()

        if entries:
            self.log.extend(entries)
            print(f"[Node {self.node_id}] Log updated → {self.log}")

        return True

    # ------------------------------------------------------
    # ------------------ ELECTION LOGIC --------------------
    # ------------------------------------------------------

    def start_election(self):
        self.state = RaftNode.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1

        print(f"\n[Node {self.node_id}] Starting election (Term {self.current_term})")

        for node in self.cluster:
            if node.node_id != self.node_id:
                vote, _ = node.request_vote(self.current_term, self.node_id)
                if vote:
                    self.votes_received += 1

        # Win election
        if self.votes_received > len(self.cluster) // 2:
            self.state = RaftNode.LEADER
            print(f"\n [Node {self.node_id}] Became LEADER (Term {self.current_term})\n")
            for node in self.cluster:
                self.next_index[node.node_id] = len(self.log)
                self.match_index[node.node_id] = 0

    # ------------------------------------------------------
    # ------------------- MAIN LOOP ------------------------
    # ------------------------------------------------------

    def run(self):
        while self.running:
            time.sleep(0.15)

            # Leader: send heartbeats
            if self.state == RaftNode.LEADER:
                for node in self.cluster:
                    if node.node_id != self.node_id:
                        node.append_entries(self.current_term, self.node_id)
                continue

            # Follower or Candidate: check for timeout
            if time.time() >= self.election_timeout:
                self.start_election()
                self.reset_election_timeout()

    def stop(self):
        self.running = False


# ------------------------------------------------------
# --------------- SIMULATION CONTROLLER -----------------
# ------------------------------------------------------

class RaftSimulation:
    def __init__(self, n=3):
        self.nodes = []

        for i in range(n):
            self.nodes.append(RaftNode(i, None))

        # Set cluster references
        for node in self.nodes:
            node.cluster = self.nodes

    def start(self):
        print("=== Starting RAFT Simulation with 3 Nodes ===")
        for node in self.nodes:
            node.start()

    def stop(self):
        for node in self.nodes:
            node.stop()

    def kill_leader(self):
        """Simulate failure of current leader."""
        for node in self.nodes:
            if node.state == RaftNode.LEADER:
                print(f"\n Simulating failure of LEADER Node {node.node_id}\n")
                node.stop()
                return node.node_id
        return None


# ------------------------------------------------------
# ---------------------- MAIN --------------------------
# ------------------------------------------------------

if __name__ == "__main__":
    sim = RaftSimulation(n=3)
    sim.start()

    # Allow time for initial leader election
    time.sleep(8)

    # Kill leader and observe re-election
    sim.kill_leader()

    # Allow time for re-election
    time.sleep(10)

    sim.stop()

    print("\n=== Simulation Finished ===")
