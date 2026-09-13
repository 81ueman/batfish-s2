#!/usr/bin/env python3
"""Partition-quality metrics for an S2 network snapshot (P0, evaluation plan §6.1).

Given a network directory (`networks/<net>/configs`) and a node->worker assignment,
this builds the union communication graph and reports the partition quality:

  * node count
  * node weights = #interfaces + #BGP peers + #origination prefixes, with the
    weight imbalance max/mean over the workers
  * weighted cut = graph edges crossing a worker boundary (weight 1 per edge, or
    an estimated weight when `--edge-weights=estimated`)

The graph is the union of:
  * L3 adjacencies: two nodes are adjacent when they have interface addresses in
    the same IPv4 subnet (this includes loopbacks only if they share a subnet);
  * BGP sessions: `neighbor <ip|hostname> remote-as <n>` resolved to the node that
    owns `<ip>` (or to `<hostname>` directly).

The default assignment reimplements `NetworkPartitioner.partition` (deterministic
seed-0 round-robin over a 64-bit hash shuffle) exactly, so the metric matches the
current Java runner. The current Java behaviour is also made explicit by running
with the same `--seed`/`--workers` that the runner uses (seed 0, demo default 3).

Usage:
  scripts/partition-metrics.py networks/s2-triangle
  scripts/partition-metrics.py --workers 3 --seed 0 networks/s2-fat4
  scripts/partition-metrics.py --workers 3 networks/s2-triangle --assignment assign.txt
  scripts/partition-metrics.py --json networks/s2-triangle

An explicit assignment file is either JSON (`{"r1": 0, "r2": 1}`) or one
`hostname worker` pair per line (`#`-comments allowed). Hosts missing from the
file are reported and assigned to worker 0.

Dependencies: Python 3 standard library only.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

MASK64 = (1 << 64) - 1


# --------------------------------------------------------------------------- Java partitioner


def _java_string_hash(value: str) -> int:
    """Java String.hashCode (32-bit signed wraparound)."""
    h = 0
    for ch in value:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000
    return h


def _mix(seed: int, hostname: str) -> int:
    """NetworkPartitioner.mix: 64-bit signed wraparound, logical shifts."""
    h = (seed * 0x9E3779B97F4A7C15 + _java_string_hash(hostname)) & MASK64
    h ^= h >> 33
    h = (h * 0xFF51AFD7ED558CCD) & MASK64
    h ^= h >> 33
    return h


def _signed64(value: int) -> int:
    return value - (1 << 64) if value >= (1 << 63) else value


def network_partitioner(hostnames, num_workers: int, seed: int):
    """Faithful Python port of NetworkPartitioner.partition (Comparator.comparingLong)."""
    ordered = sorted(hostnames, key=lambda h: _signed64(_mix(seed, h)))
    return {host: i % num_workers for i, host in enumerate(ordered)}


# --------------------------------------------------------------------------- config parsing


class Node:
    def __init__(self, hostname: str) -> None:
        self.hostname = hostname
        self.interfaces = 0
        self.peers = 0
        self.origination_prefixes = 0

    @property
    def weight(self) -> int:
        return self.interfaces + self.peers + self.origination_prefixes


def _parse_intf_address(line: str):
    """Parse `ip address <ip> <mask> [secondary]` -> IPv4Network or None."""
    parts = line.split()
    # parts[0] == "ip", parts[1] == "address"
    if len(parts) < 4:
        return None
    try:
        ip = ipaddress.IPv4Address(parts[2])
    except ipaddress.AddressValueError:
        return None
    # A prefix length (`ip address 1.1.1.1/32`) is not used by these snapshots, but accept it.
    if "/" in parts[3]:
        try:
            return ipaddress.IPv4Network(f"{ip}/{parts[3].split('/')[1]}", strict=False)
        except (ipaddress.AddressValueError, ipaddress.NetmaskValueError):
            return None
    try:
        return ipaddress.IPv4Network(f"{ip}/{parts[3]}", strict=False)
    except (ipaddress.AddressValueError, ipaddress.NetmaskValueError):
        return None


class Config:
    def __init__(self, hostname: str) -> None:
        self.node = Node(hostname)
        # subnet -> list of (hostname, ip)
        self.addresses = []  # (ip, subnet)
        self.neighbors = []  # strings (ip or hostname)


def parse_config(path: Path) -> Config:
    hostname = None
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    for raw in lines:
        if not hostname:
            m = re.match(r"\s*hostname\s+(\S+)", raw)
            if m:
                hostname = m.group(1)
    if not hostname:
        hostname = path.name
    cfg = Config(hostname)

    current_interface_has_address = False
    in_bgp = False
    for raw in lines:
        line = raw.split("!", 1)[0].strip()
        if not line:
            continue
        if line.startswith("interface "):
            current_interface_has_address = False
            continue
        if line.startswith("router "):
            in_bgp = line.startswith("router bgp ")
            continue
        if line.startswith("ip address "):
            net = _parse_intf_address(line)
            if net is not None:
                if not current_interface_has_address:
                    current_interface_has_address = True
                    cfg.node.interfaces += 1
                # Normalize the address to its subnet's network address + length.
                cfg.addresses.append((ipaddress.IPv4Address(line.split()[2]), net))
            continue
        if line.startswith("neighbor "):
            parts = line.split()
            if len(parts) >= 4 and parts[2] == "remote-as":
                cfg.node.peers += 1
                cfg.neighbors.append(parts[1])
            continue
        if in_bgp and line.startswith("network "):
            # `network <ip> mask <mask>` or `network <prefix>/<len>`
            parts = line.split()
            if len(parts) >= 4 and parts[2] == "mask":
                cfg.node.origination_prefixes += 1
            elif "/" in parts[1]:
                cfg.node.origination_prefixes += 1
            continue
    return cfg


# --------------------------------------------------------------------------- graph


def build_graph(configs):
    """Return (nodes, edges, address_owner).

    edges maps a sorted (u, v) pair to {"l3": number of shared subnets, "bgp": number of
    BGP sessions}.
    """
    nodes = {c.node.hostname: c.node for c in configs}
    address_owner = {}
    subnets = defaultdict(list)
    for cfg in configs:
        for ip, net in cfg.addresses:
            address_owner[str(ip)] = cfg.node.hostname
            subnets[str(net)].append((cfg.node.hostname, str(ip)))

    edges = defaultdict(lambda: {"l3": 0, "bgp": 0})
    for _net, members in subnets.items():
        hosts = sorted({h for h, _ip in members})
        for i in range(len(hosts)):
            for j in range(i + 1, len(hosts)):
                edges[(hosts[i], hosts[j])]["l3"] += 1
    for cfg in configs:
        for neighbor in cfg.neighbors:
            other = address_owner.get(neighbor, neighbor if neighbor in nodes else None)
            if other is None or other == cfg.node.hostname:
                continue
            pair = tuple(sorted((cfg.node.hostname, other)))
            edges[pair]["bgp"] += 1
    return nodes, dict(edges), address_owner


# --------------------------------------------------------------------------- assignment


def load_assignment(path: Path):
    text = path.read_text(encoding="utf-8")
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return {str(k): int(v) for k, v in obj.items()}
    except json.JSONDecodeError:
        pass
    mapping = {}
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        parts = re.split(r"[\s,=]+", line)
        if len(parts) < 2:
            raise ValueError(f"bad assignment line: {line!r}")
        mapping[parts[0]] = int(parts[1])
    return mapping


# --------------------------------------------------------------------------- reporting


def compute_metrics(nodes, edges, assignment, edge_weights, workers):
    hosts = sorted(nodes)
    loads = [0] * workers
    for host in hosts:
        loads[assignment[host]] += nodes[host].weight

    cut_edges = []
    cut_weight = 0.0
    for (u, v), meta in sorted(edges.items()):
        if assignment[u] == assignment[v]:
            continue
        if edge_weights == "estimated":
            w = meta["l3"] + meta["bgp"]
        else:
            w = 1
        cut_weight += w
        cut_edges.append((u, v, w, meta))

    mean = (sum(loads) / workers) if workers else 0.0
    max_load = max(loads) if loads else 0
    imbalance = (max_load / mean) if mean else 0.0
    total_edges = len(edges)
    return {
        "nodes": len(hosts),
        "workers": workers,
        "edges": total_edges,
        "l3_edges": sum(1 for m in edges.values() if m["l3"]),
        "bgp_sessions": sum(m["bgp"] for m in edges.values()),
        "cut_edges": len(cut_edges),
        "cut_weight": cut_weight,
        "cut_fraction": (len(cut_edges) / total_edges) if total_edges else 0.0,
        "loads": loads,
        "max_load": max_load,
        "mean_load": mean,
        "imbalance": imbalance,
        "per_worker": [[h for h in hosts if assignment[h] == w] for w in range(workers)],
    }


def print_report(network, configs_dir, assignment_source, nodes, edges, assignment, metrics, edge_weights, per_node_cut):
    print(f"network:      {network}")
    print(f"configs:      {configs_dir}")
    print(f"nodes:        {metrics['nodes']}")
    print(f"workers:      {metrics['workers']}")
    print(f"assignment:   {assignment_source}")
    print(
        "edges:        union={}  l3={}  bgp-sessions={}".format(
            metrics["edges"], metrics["l3_edges"], metrics["bgp_sessions"]
        )
    )
    print(
        "cut:          edges={}  weighted={} (weights={})  {:.1%} of edges".format(
            metrics["cut_edges"],
            _fmt(metrics["cut_weight"]),
            edge_weights,
            metrics["cut_fraction"],
        )
    )
    print(
        "node weight:  min={}  max={}  mean={}  total={}  imbalance(max/mean)={:.3f}".format(
            min((n.weight for n in nodes.values()), default=0),
            max((n.weight for n in nodes.values()), default=0),
            _fmt(sum(n.weight for n in nodes.values()) / len(nodes)) if nodes else "0",
            sum(n.weight for n in nodes.values()),
            metrics["imbalance"],
        )
    )
    print(
        "worker load:  "
        + "  ".join(
            f"w{w}={int(metrics['loads'][w])}"
            for w in range(metrics["workers"])
        )
    )
    if per_node_cut:
        parts = "  ".join(f"{h}:{c}" for h, c in per_node_cut if c)
        if parts:
            print(f"cut by node:  {parts}")
    print("per-worker assignment:")
    for w in range(metrics["workers"]):
        members = " ".join(
            f"{h}({nodes[h].weight})" for h in metrics["per_worker"][w]
        )
        print(f"  w{w}: {members or '(none)'}")


def _fmt(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.3f}"


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Partition-quality metrics for an S2 network snapshot.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "network",
        help="network directory (networks/<net>) or its configs directory",
    )
    parser.add_argument("--workers", type=int, default=None, help="number of workers (default 3)")
    parser.add_argument("--seed", type=int, default=0, help="NetworkPartitioner shuffle seed (default 0)")
    parser.add_argument(
        "--assignment",
        type=Path,
        default=None,
        help="explicit hostname->worker mapping (JSON or 'host worker' lines)",
    )
    parser.add_argument(
        "--edge-weights",
        choices=("uniform", "estimated"),
        default="uniform",
        help="cut edge weight: 1 per edge (uniform) or l3+bgp count (estimated)",
    )
    parser.add_argument("--json", action="store_true", help="emit the metrics as JSON")
    args = parser.parse_args(argv)

    network = Path(args.network)
    configs_dir = network / "configs" if (network / "configs").is_dir() else network
    if not configs_dir.is_dir():
        parser.error(f"not a directory: {configs_dir}")

    configs = [parse_config(p) for p in sorted(configs_dir.iterdir()) if p.is_file() and not p.name.startswith(".")]
    if not configs:
        parser.error(f"no config files under {configs_dir}")

    nodes, edges, _address_owner = build_graph(configs)
    hosts = set(nodes)

    if args.assignment is not None:
        assignment = load_assignment(args.assignment)
        missing = sorted(hosts - set(assignment))
        unknown = sorted(set(assignment) - hosts)
        if missing:
            print(f"warning: {len(missing)} host(s) missing from assignment, defaulting to w0: {missing}", file=sys.stderr)
            for h in missing:
                assignment[h] = 0
        if unknown:
            print(f"warning: assignment has unknown host(s), ignoring: {unknown}", file=sys.stderr)
        assignment = {h: assignment[h] for h in hosts}
        workers = max(assignment.values()) + 1
        if args.workers:
            workers = max(workers, args.workers)
        source = f"file {args.assignment}"
    else:
        workers = args.workers if args.workers else 3
        if workers < 1:
            parser.error("--workers must be >= 1")
        assignment = network_partitioner(hosts, workers, args.seed)
        source = f"NetworkPartitioner(seed={args.seed}, workers={workers})"

    metrics = compute_metrics(nodes, edges, assignment, args.edge_weights, workers)

    per_node_cut = []
    for host in sorted(hosts):
        c = sum(
            1 for (u, v) in edges if (u == host or v == host) and assignment[u] != assignment[v]
        )
        per_node_cut.append((host, c))

    if args.json:
        out = {
            "network": network.name,
            "configs": str(configs_dir),
            "assignment": source,
            "edge_weights": args.edge_weights,
            "metrics": {k: v for k, v in metrics.items() if k != "per_worker"},
            "assignment_map": assignment,
        }
        print(json.dumps(out, indent=2, sort_keys=True))
    else:
        print_report(
            network.name,
            configs_dir,
            source,
            nodes,
            edges,
            assignment,
            metrics,
            args.edge_weights,
            per_node_cut,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
