#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Generate S2 testbed snapshots (Cisco-style configs) for partition/scale evaluation.

Usage:
  scripts/gen-topology.py fattree --k 4 [--originate 2] --out networks/s2-fat4
  scripts/gen-topology.py line --nodes 8 [--originate 4] --out networks/s2-line8
  scripts/gen-topology.py hub --spokes 8 [--originate 2] --out networks/s2-hub

Every switch is its own eBGP AS and originates its loopback /32; edge switches additionally
originate --originate extra /32 loopbacks each, to control the route count.
"""

import argparse
import os
import shutil


def _iface(idx):
    return "GigabitEthernet0/%d" % idx


def _extra_prefix(octet, i):
    """The i-th extra /32 originated by the switch whose octet is ``octet``.

    ``10.<octet>.<i//256>.<i%256>`` keeps prefixes unique per switch for up to 65536 extra
    originations (a plain ``i+1`` fourth octet overflows past 254), matching networks/s2-giga.
    """
    return "10.%d.%d.%d" % (octet, i // 256, i % 256)


def _write_config(path, name, asn, loopback, ifaces, extra_originations, neighbors):
    """ifaces: list of (name, ip, mask); neighbors: list of (peer_ip, peer_asn, iface_name)."""
    lines = ["hostname %s" % name, ""]
    lines += ["interface Loopback0", " ip address %s 255.255.255.255" % loopback, ""]
    for i in range(extra_originations):
        # A /32 unique per switch and per host.
        octet = int(loopback.split(".")[2])
        lines += [
            "interface Loopback%d" % (i + 1),
            " ip address %s 255.255.255.255" % _extra_prefix(octet, i),
            "",
        ]
    for name_, ip, mask in ifaces:
        lines += ["interface %s" % name_, " ip address %s %s" % (ip, mask), ""]
    lines += ["router bgp %d" % asn, " bgp router-id %s" % loopback]
    lines.append(" network %s mask 255.255.255.255" % loopback)
    for i in range(extra_originations):
        octet = int(loopback.split(".")[2])
        lines.append(" network %s mask 255.255.255.255" % _extra_prefix(octet, i))
    for peer_ip, peer_asn, _ in neighbors:
        lines.append(" neighbor %s remote-as %d" % (peer_ip, peer_asn))
    lines.append("")
    with open(path, "w") as fh:
        fh.write("\n".join(lines))


def _emit(out, count, links, kinds, loopbacks, asns, extra_by_id):
    configs = os.path.join(out, "configs")
    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(configs)
    # Assign each link a /30: 10.<1+i//256>.<i%256>.0/30, endpoints .1 and .2.
    link_ips = {}
    for i, (a, b) in enumerate(links):
        base = "10.%d.%d" % (1 + i // 256, i % 256)
        link_ips[(a, b)] = (base + ".1", base + ".2")
    # Per-switch interfaces / neighbors.
    ifaces = {s: [] for s in range(count)}
    neighbors = {s: [] for s in range(count)}
    iface_idx = {s: 0 for s in range(count)}
    for (a, b), (ipa, ipb) in link_ips.items():
        na, nb = _iface(iface_idx[a]), _iface(iface_idx[b])
        iface_idx[a] += 1
        iface_idx[b] += 1
        ifaces[a].append((na, ipa, "255.255.255.252"))
        ifaces[b].append((nb, ipb, "255.255.255.252"))
        neighbors[a].append((ipb, asns[b], na))
        neighbors[b].append((ipa, asns[a], nb))
    for s in range(count):
        _write_config(
            os.path.join(configs, "sw%d" % s),
            "sw%d" % s,
            asns[s],
            loopbacks[s],
            ifaces[s],
            extra_by_id.get(s, 0),
            neighbors[s],
        )


def gen_fattree(k, originate, out):
    half = k // 2
    ids = {}
    n = 0
    for c1 in range(half):
        for c2 in range(half):
            ids[("core", c1, c2)] = n
            n += 1
    for p in range(k):
        for j in range(half):
            ids[("agg", p, j)] = n
            n += 1
    for p in range(k):
        for j in range(half):
            ids[("edge", p, j)] = n
            n += 1
    links = []
    for p in range(k):
        for e in range(half):
            for a in range(half):
                links.append((ids[("edge", p, e)], ids[("agg", p, a)]))
    for p in range(k):
        for a in range(half):
            for c2 in range(half):
                links.append((ids[("agg", p, a)], ids[("core", a, c2)]))
    loopbacks = ["10.0.%d.1" % (s + 1) for s in range(n)]
    asns = [65000 + s for s in range(n)]
    extra = {s: originate for key, s in ids.items() if key[0] == "edge"}
    _emit(out, n, links, ids, loopbacks, asns, extra)
    return n, len(links)


def gen_line(nodes, originate, out):
    links = [(i, i + 1) for i in range(nodes - 1)]
    loopbacks = ["10.0.%d.1" % (s + 1) for s in range(nodes)]
    asns = [65000 + s for s in range(nodes)]
    extra = {s: originate for s in range(nodes)}
    _emit(out, nodes, links, None, loopbacks, asns, extra)
    return nodes, len(links)


def gen_hub(spokes, originate, out):
    """A hub/route-reflector star: one central node with every leaf as a peer.

    The hub (node 0) has ``spokes`` interfaces and BGP peers; each leaf has one. This is a WAN
    shape whose busiest tier already has at least as many interfaces as the sparse tier, so it
    exercises the adaptive role rule's ``peerCoefficient = 0`` branch on a non-FatTree topology.
    """
    count = spokes + 1
    links = [(0, s) for s in range(1, count)]
    loopbacks = ["10.0.%d.1" % (s + 1) for s in range(count)]
    asns = [65000 + s for s in range(count)]
    extra = {s: originate for s in range(1, count)}
    _emit(out, count, links, None, loopbacks, asns, extra)
    return count, len(links)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="kind", required=True)
    ft = sub.add_parser("fattree")
    ft.add_argument("--k", type=int, required=True)
    ft.add_argument("--originate", type=int, default=0)
    ft.add_argument("--out", required=True)
    ln = sub.add_parser("line")
    ln.add_argument("--nodes", type=int, required=True)
    ln.add_argument("--originate", type=int, default=0)
    ln.add_argument("--out", required=True)
    hb = sub.add_parser("hub")
    hb.add_argument("--spokes", type=int, required=True)
    hb.add_argument("--originate", type=int, default=0)
    hb.add_argument("--out", required=True)
    args = ap.parse_args()
    if args.kind == "fattree":
        n, e = gen_fattree(args.k, args.originate, args.out)
        print("fattree k=%d: %d switches, %d links -> %s" % (args.k, n, e, args.out))
    elif args.kind == "hub":
        n, e = gen_hub(args.spokes, args.originate, args.out)
        print("hub spokes=%d: %d switches, %d links -> %s" % (args.spokes, n, e, args.out))
    else:
        n, e = gen_line(args.nodes, args.originate, args.out)
        print("line: %d switches, %d links -> %s" % (n, e, args.out))


if __name__ == "__main__":
    main()
