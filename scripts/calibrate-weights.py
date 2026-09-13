#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Calibrate the S2 partitioner's node-weight model against measured per-node cost (O6).

The Java partitioner ({@code NodeWeights}) scores each router with an additive feature sum so
that {@code WEIGHTED_LPT_FM} can balance the per-worker load before simulation. This script fits
that score to a **measured** per-node cost from real runs.

Measurement (see {@code docs/s2-port/PARTITIONING-PLAN.md} section 6.7):

* **Features** come from the parsed configuration, dumped exactly as the partitioner sees them by
  running the offline partition role with the calibration hook:

      java -Ds2.nodeWeightsDump=features-<net>.tsv \\
          -jar bazel-bin/projects/s2/s2_main_deploy.jar partition <net> <workers>

* **Measured cost** is the per-node main-RIB route count, parsed from the controller's
  {@code results/local-<net>-<workers>/result-<workers>worker.txt} (the {@code --- distributed ---}
  section). The main RIB is the dominant retained per-node term that the partitioner balances
  ({@code R_w} in the plan's cost model). It is a deterministic, per-node quantity, whereas the
  per-worker peak heap is dominated by the worker-independent configuration floor at the sizes we
  can run here.

Modes:

  fit        Pool every supplied sample, fit the coefficients with non-negative ridge least
             squares, and report the before/after correlation between the predicted weight and the
             measured route count.
  imbalance  Given an assignment file (or the Java default) and the measured costs, report the
             predicted-vs-measured per-worker load imbalance (max/mean) that the partition achieves.

A *sample* is {@code NAME:RESULT_FILE:FEATURES_FILE}, e.g.

  scripts/calibrate-weights.py fit \\
      --sample s2-line:results/local-s2-line-1/result-1worker.txt:results/calib/features-s2-line.tsv \\
      --sample s2-fat4:results/local-s2-fat4-1/result-1worker.txt:results/calib/features-s2-fat4.tsv

Dependencies: Python 3 standard library only.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path

# Route classes that can appear as top-level main-RIB entries in the controller's result dump.
ROUTE_CLASSES = (
    "Bgpv4Route",
    "ConnectedRoute",
    "LocalRoute",
    "StaticRoute",
    "OspfRoute",
    "OspfExternalType1Route",
    "OspfExternalType2Route",
    "OspfExternalRoute",
    "KernelRoute",
    "RipRoute",
    "EvpnRoute",
)

FEATURE_COLUMNS = (
    "interfaces",
    "peers",
    "originationPrefixes",
    "aclLines",
    "policyStatements",
    "staticRoutes",
    "vrfs",
)

# The v1 model: every coefficient is 1.
V1_COEFFICIENTS = {name: 1 for name in FEATURE_COLUMNS}


# --------------------------------------------------------------------------- inputs


def parse_result_routes(path: Path):
    """Return {hostname: main-RIB route count} from a controller result file."""
    text = path.read_text(encoding="utf-8", errors="replace")
    if "--- distributed ---\n" not in text:
        raise ValueError(f"no distributed RIB section in {path}")
    body = text.split("--- distributed ---\n", 1)[1].strip()
    # The dump is a Java `Map.toString()`: {host={vrf=[Route{...}, ...]}, ...}. Track brace depth
    # to split the top-level host entries (each host value is a `{...}` at depth 1), then count the
    # route-class tokens inside each host.
    depth = 0
    start = 1  # skip the outer `{`
    segments = []
    for idx, ch in enumerate(body):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 1:
                segments.append(body[start : idx + 1])
                start = idx + 3  # skip "}, "
    counts = {}
    for segment in segments:
        match = re.match(r"\s*([^=]+)=\{(.*)\}\s*$", segment.strip().rstrip(","), re.S)
        if not match:
            continue
        host = match.group(1).strip()
        host_body = match.group(2)
        count = sum(
            len(re.findall(r"\b" + re.escape(cls) + r"[<{]", host_body)) for cls in ROUTE_CLASSES
        )
        counts[host] = count
    return counts


def parse_features(path: Path):
    """Return ({hostname: {feature: value}}, ordered feature names) from a NodeWeights TSV dump."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ValueError(f"empty feature dump: {path}")
    header = lines[0].split("\t")
    if header[0] != "hostname":
        raise ValueError(f"bad feature dump header in {path}: {header}")
    columns = header[1:]
    # The Java dump appends the model's current total as a trailing `weight` column for humans;
    # it is a derived value, not a feature, so drop it from the design matrix.
    if columns and columns[-1] == "weight":
        columns = columns[:-1]
    features = {}
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        host = parts[0]
        features[host] = {col: int(val) for col, val in zip(columns, parts[1 : 1 + len(columns)])}
    return features, columns


def load_samples(sample_specs):
    """Return a list of samples: {name, result, features_file, hosts, measures, columns}."""
    samples = []
    for spec in sample_specs:
        parts = spec.split(":")
        if len(parts) != 3:
            raise SystemExit(f"--sample must be NAME:RESULT:FEATURES, got {spec!r}")
        name, result_path, features_path = parts
        samples.append(load_sample(name, Path(result_path), Path(features_path)))
    return samples


def load_sample(name, result_path, features_path):
    features, columns = parse_features(features_path)
    routes = parse_result_routes(result_path)
    hosts = sorted(set(features) & set(routes))
    missing_features = sorted(set(routes) - set(features))
    missing_routes = sorted(set(features) - set(routes))
    if missing_features or missing_routes:
        print(
            f"warning: {name}: features-only={missing_routes} routes-only={missing_features}",
            file=sys.stderr,
        )
    return {
        "name": name,
        "result": str(result_path),
        "features_file": str(features_path),
        "hosts": hosts,
        "columns": columns,
        "features": features,
        "cost": {host: routes[host] for host in hosts},
    }


# --------------------------------------------------------------------------- statistics


def dot(weights, vector, columns):
    return sum(weights.get(col, 0) * vector.get(col, 0) for col in columns)


def pearson(xs, ys):
    n = len(xs)
    if n < 2:
        return float("nan")
    mx = sum(xs) / n
    my = sum(ys) / n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx == 0 or syy == 0:
        return float("nan")
    return sxy / math.sqrt(sxx * syy)


def _rank(values):
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        average = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = average
        i = j + 1
    return ranks


def spearman(xs, ys):
    return pearson(_rank(xs), _rank(ys))


def ridge_fit(rows, columns, lam):
    """Non-negative ridge least squares: min ||A b - y||^2 + lam ||b||^2, b >= 0.

    Implemented as projected gradient descent on the smooth objective, which keeps every
    coefficient non-negative (a negative weight would be nonsensical for a load estimate).
    """
    n_features = len(columns)
    n = len(rows)
    if n == 0:
        raise SystemExit("no samples to fit")
    # Normalize columns to comparable scale so one step size works; recover the scale afterwards.
    scales = []
    for col in columns:
        rms = math.sqrt(sum(row["x"].get(col, 0) ** 2 for row in rows) / n)
        scales.append(rms if rms > 0 else 1.0)
    a = [[row["x"].get(col, 0) / scales[j] for j, col in enumerate(columns)] for row in rows]
    y = [row["y"] for row in rows]
    beta = [0.0] * n_features
    # Lipschitz constant of the gradient: largest eigenvalue of A^T A + lam I is <= trace + lam.
    trace = sum(sum(a[i][j] ** 2 for i in range(n)) for j in range(n_features))
    step = 1.0 / max(trace + lam, 1e-9)
    for _ in range(20000):
        grad = [
            sum((sum(a[i][j] * beta[j] for j in range(n_features)) - y[i]) * a[i][j] for i in range(n))
            + lam * beta[j]
            for j in range(n_features)
        ]
        new_beta = [max(0.0, beta[j] - step * grad[j]) for j in range(n_features)]
        delta = max(abs(new_beta[j] - beta[j]) for j in range(n_features))
        beta = new_beta
        if delta < 1e-12:
            break
    return {columns[j]: beta[j] / scales[j] for j in range(n_features)}


def integerize(coefficients, columns, varying=None, prior=1):
    """Scale non-negative coefficients to small positive integers (weight scale is irrelevant).

    The partitioner only compares weights, so any positive scaling is equivalent. Zero
    coefficients become 0; the smallest positive coefficient becomes 1. A feature with no variation
    in the calibration dataset is *unidentifiable*: the regression cannot say anything about it, so
    it gets the documented ``prior`` (1, the v1 value) rather than an arbitrary fitted 0.
    """
    positive = [coefficients[c] for c in columns if coefficients[c] > 0]
    if not positive:
        scaled = {c: 0.0 for c in columns}
    else:
        unit = min(positive)
        scaled = {c: coefficients[c] / unit for c in columns}
        top = max(scaled.values())
        if top > 1000:
            scaled = {c: v * 1000 / top for c, v in scaled.items()}
    result = {c: int(round(scaled[c])) for c in columns}
    if varying is not None:
        for col in columns:
            if col not in varying:
                result[col] = prior
    return result


# --------------------------------------------------------------------------- reporting


def dataset_vectors(samples, columns):
    xs, ys = [], []
    for sample in samples:
        for host in sample["hosts"]:
            xs.append(sample["features"][host])
            ys.append(sample["cost"][host])
    return xs, ys


def centered_rows(samples, columns):
    """Rows of (feature - per-network mean, cost - per-network mean).

    Pooling raw nodes lets the between-network scale dominate the fit; the partitioner only
    compares nodes *within* one network, so the calibration target is the within-network marginal
    cost. Centering per network removes the network fixed effect (equivalently, fits a per-network
    intercept) and identifies the coefficients from the node-to-node variation the partitioner
    actually uses.
    """
    rows = []
    for sample in samples:
        hosts = sample["hosts"]
        n = len(hosts)
        if n == 0:
            continue
        mean_x = {
            col: sum(sample["features"][h].get(col, 0) for h in hosts) / n for col in columns
        }
        mean_y = sum(sample["cost"][h] for h in hosts) / n
        for host in hosts:
            rows.append(
                {
                    "x": {
                        col: sample["features"][host].get(col, 0) - mean_x[col] for col in columns
                    },
                    "y": sample["cost"][host] - mean_y,
                }
            )
    return rows


def print_correlations(samples, columns, coefficients, title):
    print(title)
    print("  network           n   pearson  spearman")
    xs, ys = dataset_vectors(samples, columns)
    pooled = [dot(coefficients, x, columns) for x in xs]
    print(
        f"  {'ALL (pooled)':<16} {len(xs):>3}   {pearson(pooled, ys):>7.3f}  "
        f"{spearman(pooled, ys):>8.3f}"
    )
    for sample in samples:
        sx = [sample["features"][h] for h in sample["hosts"]]
        sy = [sample["cost"][h] for h in sample["hosts"]]
        pred = [dot(coefficients, x, columns) for x in sx]
        print(
            f"  {sample['name']:<16} {len(sx):>3}   {pearson(pred, sy):>7.3f}  "
            f"{spearman(pred, sy):>8.3f}"
        )


def print_fit(samples, columns, coefficients, varying):
    print("fitted coefficients (non-negative ridge least squares):")
    for col in columns:
        note = "" if col in varying else "  (unidentifiable: constant in the dataset)"
        print(f"  {col:<22} {coefficients[col]:>12.6f}{note}")
    print("\nrecommended integer coefficients:")
    ints = integerize(coefficients, columns, varying)
    for col in columns:
        print(f"  {col:<22} {ints[col]:>12d}")
    print()
    print_correlations(samples, columns, ints, "correlation of weight with measured route count:")


def load_assignment(path: Path):
    mapping = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        parts = re.split(r"[\s,=]+", line)
        mapping[parts[0]] = int(parts[1])
    return mapping


def report_imbalance(samples, assignment, workers, title):
    """Report the measured per-worker cost and its max/mean imbalance for an explicit assignment.

    The *measured* metric is the per-node main-RIB route count aggregated per worker, so it does not
    depend on the weight model at all: it compares how a given assignment balances real cost.
    """
    print(f"\n{title}")
    for sample in samples:
        costs = [0] * workers
        members = [[] for _ in range(workers)]
        for host in sample["hosts"]:
            w = assignment.get(host)
            if w is None:
                continue
            costs[w] += sample["cost"][host]
            members[w].append(host)
        print(f"  {sample['name']:<14} measured cost: {costs}  imbalance={imbalance(costs):.3f}")
    return {
        sample["name"]: {
            "costs": costs,
            "imbalance": imbalance(costs),
        }
        for sample in samples
    }


def imbalance(loads):
    positive = [x for x in loads]
    if not positive:
        return float("nan")
    mean = sum(positive) / len(positive)
    if mean == 0:
        return float("nan")
    return max(positive) / mean


# --------------------------------------------------------------------------- main


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Calibrate the S2 node-weight model against measured per-node cost.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    fit = sub.add_parser("fit", help="fit coefficients and report correlations")
    fit.add_argument(
        "--sample",
        action="append",
        required=True,
        metavar="NAME:RESULT:FEATURES",
        help="one measured sample; repeatable",
    )
    fit.add_argument("--lambda", dest="lam", type=float, default=1.0, help="ridge lambda")
    fit.add_argument(
        "--center",
        action="store_true",
        help="remove the per-network mean (within-network calibration; the partitioner's regime)",
    )
    fit.add_argument("--json", action="store_true")

    imb = sub.add_parser("imbalance", help="measured per-worker cost imbalance of assignments")
    imb.add_argument(
        "--sample",
        action="append",
        required=True,
        metavar="NAME:RESULT:FEATURES",
        help="one measured sample; repeatable",
    )
    imb.add_argument(
        "--assignment",
        action="append",
        required=True,
        metavar="LABEL=PATH",
        help="an assignment file to score; repeatable",
    )
    imb.add_argument("--workers", type=int, required=True)

    args = parser.parse_args(argv)
    samples = load_samples(args.sample)

    if args.mode == "fit":
        columns = samples[0]["columns"]
        if args.center:
            rows = centered_rows(samples, columns)
        else:
            xs, ys = dataset_vectors(samples, columns)
            rows = [{"x": x, "y": y} for x, y in zip(xs, ys)]
        print(f"samples: {len(samples)}  nodes: {len(rows)}")
        for sample in samples:
            print(f"  {sample['name']}: {len(sample['hosts'])} nodes")
        print()
        print_correlations(samples, columns, V1_COEFFICIENTS, "v1 (all coefficients = 1):")
        coefficients = ridge_fit(rows, columns, args.lam)
        varying = {
            col for col in columns if len({row["x"].get(col, 0) for row in rows}) > 1
        }
        print()
        print_fit(samples, columns, coefficients, varying)
        ints = integerize(coefficients, columns, varying)
        if args.json:
            print(
                json.dumps(
                    {
                        "measured": "main-rib-route-count",
                        "coefficients": coefficients,
                        "integerCoefficients": ints,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        return 0

    if args.mode == "imbalance":
        for spec in args.assignment:
            if "=" not in spec:
                raise SystemExit(f"--assignment must be LABEL=PATH, got {spec!r}")
            label, path = spec.split("=", 1)
            assignment = load_assignment(Path(path))
            report_imbalance(samples, assignment, args.workers, f"assignment {label} ({path})")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
