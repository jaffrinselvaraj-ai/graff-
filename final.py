
import math
import random
import time
import heapq
import statistics
from typing import Dict, Tuple, List, Set, Optional


class Graph:
    """Generic weighted directed graph storing node coordinates for heuristics."""

    def __init__(self):
        # adjacency: node -> list of (neighbor, weight)
        self.adj: Dict[int, List[Tuple[int, float]]] = {}
        # coordinates: node -> (x, y)
        self.coords: Dict[int, Tuple[float, float]] = {}

    def add_node(self, node: int, coord: Tuple[float, float]) -> None:
        if node in self.coords:
            return
        self.coords[node] = coord
        self.adj.setdefault(node, [])

    def add_edge(self, u: int, v: int, weight: float) -> None:
        # directed edge u -> v
        if u not in self.adj:
            raise KeyError(f"Node {u} not present in graph")
        if v not in self.adj:
            raise KeyError(f"Node {v} not present in graph")
        self.adj[u].append((v, float(weight)))

    def neighbors(self, u: int) -> List[Tuple[int, float]]:
        return self.adj.get(u, [])

    def nodes(self) -> List[int]:
        return list(self.adj.keys())


# ---------------------- Algorithms ----------------------

def dijkstra(graph: Graph, source: int, target: int) -> Tuple[Optional[float], Dict[int, int], int]:
    """
    Standard Dijkstra using binary heap (heapq).
    Returns (distance to target or None), predecessor map, node_expansions (pops)
    """
    dist: Dict[int, float] = {node: math.inf for node in graph.nodes()}
    prev: Dict[int, int] = {}
    dist[source] = 0.0

    heap: List[Tuple[float, int]] = [(0.0, source)]
    visited: Set[int] = set()
    node_expansions = 0

    while heap:
        d_u, u = heapq.heappop(heap)
        # skip stale entries
        if d_u != dist[u]:
            continue
        node_expansions += 1
        if u == target:
            return dist[target], prev, node_expansions

        for v, w in graph.neighbors(u):
            alt = d_u + w
            if alt < dist[v]:
                dist[v] = alt
                prev[v] = u
                heapq.heappush(heap, (alt, v))

    return (None, prev, node_expansions)


def euclidean(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])


def astar(graph: Graph, source: int, target: int) -> Tuple[Optional[float], Dict[int, int], int]:
    """
    A* search using Euclidean distance on stored coordinates as admissible heuristic.
    Returns (distance to target or None), predecessor map, node_expansions (pops)
    """
    if source == target:
        return 0.0, {}, 0

    coords = graph.coords
    h = lambda n: euclidean(coords[n], coords[target])

    g_score: Dict[int, float] = {node: math.inf for node in graph.nodes()}
    f_score: Dict[int, float] = {node: math.inf for node in graph.nodes()}
    prev: Dict[int, int] = {}

    g_score[source] = 0.0
    f_score[source] = h(source)

    # heap of (f_score, g_score, node) - include g to break ties preferring lower g
    heap: List[Tuple[float, float, int]] = [(f_score[source], g_score[source], source)]
    node_expansions = 0

    while heap:
        f_u, g_u, u = heapq.heappop(heap)
        # skip stale
        if f_u != f_score[u] or g_u != g_score[u]:
            continue
        node_expansions += 1
        if u == target:
            return g_score[target], prev, node_expansions

        for v, w in graph.neighbors(u):
            tentative_g = g_score[u] + w
            if tentative_g < g_score[v]:
                prev[v] = u
                g_score[v] = tentative_g
                f_score[v] = tentative_g + h(v)
                heapq.heappush(heap, (f_score[v], g_score[v], v))

    return None, prev, node_expansions


# ---------------------- Graph Generators ----------------------

def grid_graph(rows: int, cols: int, spacing: float = 1.0, directed: bool = False) -> Graph:
    """Generate a grid graph (rows x cols). Nodes laid out with coordinates. Edges to neighbors with weight = Euclidean distance."""
    g = Graph()
    node_id = 0
    id_map = {}
    for r in range(rows):
        for c in range(cols):
            g.add_node(node_id, (c * spacing, r * spacing))
            id_map[(r, c)] = node_id
            node_id += 1

    for r in range(rows):
        for c in range(cols):
            u = id_map[(r, c)]
            for dr, dc in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
                nr, nc = r + dr, c + dc
                if 0 <= nr < rows and 0 <= nc < cols:
                    v = id_map[(nr, nc)]
                    w = euclidean(g.coords[u], g.coords[v])
                    g.add_edge(u, v, w)
                    if not directed:
                        g.add_edge(v, u, w)
    return g


def random_graph_er(n: int, p: float, xy_range: float = 100.0, directed: bool = False) -> Graph:
    """Erdos-Renyi random graph with nodes placed uniformly in coordinate box. Edge weights proportional to Euclidean distance with a small randomness factor."""
    g = Graph()
    for i in range(n):
        x = random.uniform(0, xy_range)
        y = random.uniform(0, xy_range)
        g.add_node(i, (x, y))

    for u in range(n):
        for v in range(u + 1, n):
            if random.random() < p:
                w = euclidean(g.coords[u], g.coords[v]) * (1.0 + random.uniform(-0.1, 0.1))
                g.add_edge(u, v, w)
                if not directed:
                    g.add_edge(v, u, w)
    return g


def scale_free_graph(n: int, m: int = 2, xy_range: float = 100.0, directed: bool = False) -> Graph:
    """Simple preferential attachment (Barabasi-Albert style) scale-free generator.
    Start with m nodes fully connected, then add nodes attaching to m existing nodes with probability proportional to degree.
    """
    if m < 1 or m >= n:
        raise ValueError("m must satisfy 1 <= m < n")

    g = Graph()
    for i in range(n):
        g.add_node(i, (random.uniform(0, xy_range), random.uniform(0, xy_range)))

    # start with a small connected clique of size m
    targets = list(range(m))
    for u in range(m):
        for v in range(u + 1, m):
            w = euclidean(g.coords[u], g.coords[v])
            g.add_edge(u, v, w)
            if not directed:
                g.add_edge(v, u, w)

    # degree list for preferential attachment
    degree = [0] * n
    for u in range(m):
        degree[u] = m - 1

    for new_node in range(m, n):
        # build attachment probabilities
        # choose m distinct existing nodes with probability proportional to degree (plus 1 to avoid zeros)
        existing = list(range(new_node))
        weights = [degree[i] + 1 for i in existing]
        # sample m distinct
        chosen = set()
        while len(chosen) < m and existing:
            pick = random.choices(existing, weights=weights, k=1)[0]
            chosen.add(pick)
        for target in chosen:
            w = euclidean(g.coords[new_node], g.coords[target])
            g.add_edge(new_node, target, w)
            if not directed:
                g.add_edge(target, new_node, w)
            degree[new_node] += 1
            degree[target] += 1

    return g


# ---------------------- Testing Harness ----------------------

def test_on_graph(graph: Graph, name: str, pairs: List[Tuple[int, int]]):
    print(f"\n=== Testing on {name} | nodes={len(graph.nodes())} | pairs={len(pairs)} ===")

    dijkstra_times = []
    dijkstra_expansions = []
    astar_times = []
    astar_expansions = []

    for idx, (s, t) in enumerate(pairs, 1):
        # Dijkstra
        t0 = time.perf_counter()
        dist_dij, prev_dij, exp_dij = dijkstra(graph, s, t)
        t1 = time.perf_counter()
        dijkstra_times.append(t1 - t0)
        dijkstra_expansions.append(exp_dij)

        # A*
        t0 = time.perf_counter()
        dist_ast, prev_ast, exp_ast = astar(graph, s, t)
        t1 = time.perf_counter()
        astar_times.append(t1 - t0)
        astar_expansions.append(exp_ast)

        print(f"Pair {idx}: {s}->{t} | Dijkstra: dist={dist_dij} exp={exp_dij} time={dijkstra_times[-1]:.6f}s | "
              f"A*: dist={dist_ast} exp={exp_ast} time={astar_times[-1]:.6f}s")

    def stats(lst):
        return (sum(lst) / len(lst), statistics.median(lst), min(lst), max(lst))

    print("\nSummary:")
    print("Dijkstra time avg/median/min/max (s):", stats(dijkstra_times))
    print("Dijkstra expansions avg/median/min/max:", stats(dijkstra_expansions))
    print("A* time avg/median/min/max (s):", stats(astar_times))
    print("A* expansions avg/median/min/max:", stats(astar_expansions))


def generate_random_pairs(nodes: List[int], k: int, ensure_connected: bool = False) -> List[Tuple[int, int]]:
    pairs = []
    n = len(nodes)
    if n < 2:
        return pairs
    while len(pairs) < k:
        a, b = random.sample(nodes, 2)
        pairs.append((a, b))
    return pairs


def main():
    random.seed(42)

    # Target total nodes ~500+. Adjust grid dims accordingly
    n_target = 500

    # Grid: choose rows*cols >= n_target
    rows = 20
    cols = 25
    if rows * cols < n_target:
        cols = math.ceil(n_target / rows)

    grid = grid_graph(rows, cols, spacing=1.0)
    nodes_grid = grid.nodes()
    pairs_grid = generate_random_pairs(nodes_grid, 10)

    # Random ER graph: p chosen to give average degree ~6
    n = n_target
    desired_avg_deg = 6
    p = desired_avg_deg / (n - 1)
    er = random_graph_er(n, p, xy_range=500.0)
    nodes_er = er.nodes()
    pairs_er = generate_random_pairs(nodes_er, 10)

    # Scale-free graph
    sf = scale_free_graph(n, m=3, xy_range=500.0)
    nodes_sf = sf.nodes()
    pairs_sf = generate_random_pairs(nodes_sf, 10)

    # Run tests
    test_on_graph(grid, f"Grid {rows}x{cols}", pairs_grid)
    test_on_graph(er, "Random ER", pairs_er)
    test_on_graph(sf, "Scale-free (PA)", pairs_sf)


if __name__ == "__main__":
    main()
