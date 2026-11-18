"""Exact-rational reference for the throughput schedule maths.

Regenerate: python3 gen_schedule_fixtures.py schedule_fixtures.json
Used by schedule_test.go; other implementations can pin to the same file."""
import json, sys, math
from fractions import Fraction as F

EPS = F(1, 10**9)

def mk(start, stages):
    segs, prev = [], start
    for to, d in stages:
        segs.append((prev, to, d)); prev = to
    return segs

def bps(segs):
    out, acc = [F(0)], F(0)
    for _, _, d in segs:
        acc += d; out.append(acc)
    return out

def rate(segs, t):
    B = bps(segs)
    if t < 0 or t >= B[-1]: return F(0)
    for k, (a, b, d) in enumerate(segs):
        if B[k] <= t < B[k+1]:
            return F(a) + F(b - a) * (t - B[k]) / d
def slope(segs, t):
    B = bps(segs)
    if t < 0 or t >= B[-1]: return F(0)
    for k, (a, b, d) in enumerate(segs):
        if B[k] <= t < B[k+1]: return F(b - a, d)

def A(segs, t):
    B = bps(segs); t = max(F(0), min(t, B[-1])); tot = F(0)
    for k, (a, b, d) in enumerate(segs):
        lo, hi = B[k], min(B[k+1], t)
        if hi <= lo: continue
        r0 = rate(segs, lo); r1 = F(a) + F(b - a) * (hi - B[k]) / d
        tot += (hi - lo) * (r0 + r1) / 2
    return tot

def W(segs, t): return A(segs, t) - A(segs, t - 1)

def L(segs, t):
    B = bps(segs)
    cuts = {t, t + 1}
    for b in B:
        for c in (b, b + 1):
            if t < c < t + 1: cuts.add(c)
    cuts = sorted(cuts); cand = list(cuts)
    for u, v in zip(cuts, cuts[1:]):
        m = (u + v) / 2
        f = rate(segs, m) - rate(segs, m - 1)
        g = slope(segs, m) - slope(segs, m - 1)
        if g != 0:
            r = m - f / g
            if u < r < v: cand.append(r)
    mn = min(W(segs, c) for c in cand)
    # dense cross-check that the candidate min is the true min
    for i in range(0, 2001):
        assert W(segs, t + F(i, 2000)) >= mn
    return math.floor(mn + EPS)

CASES = [
    ("flat hold 10 rps", 10, [(10, 60)]),
    ("ramp 0 to 50", 0, [(50, 60)]),
    ("T13 ramp hold ramp-down", 0, [(50, 60), (50, 120), (0, 30)]),
    ("spec example 0-200-200-800-0", 0, [(200, 300), (200, 600), (800, 300), (0, 120)]),
    ("falling 80 to 0", 80, [(0, 120)]),
    ("fractional per-pod 0 to 3 then hold", 0, [(3, 10), (3, 10)]),
    ("V shape", 100, [(0, 5), (100, 5)]),
    ("start above zero ramp up", 20, [(100, 40)]),
    ("1s jump then hold", 0, [(10, 1), (10, 60)]),
    ("repeated holds then ramp", 30, [(30, 10), (30, 10), (60, 10)]),
    ("ends nonzero fractional", 0, [(7, 20)]),
    ("zigzag", 0, [(40, 3), (10, 2), (40, 3), (0, 4)]),
]

out = {"epsilon": 1e-9, "cases": []}
for name, start, stages in CASES:
    segs = mk(start, stages); B = bps(segs); T = B[-1]
    ts = {F(-3, 2), F(-1, 4), F(0), F(1, 4), F(1, 2), F(1), T - F(1, 2), T, T + F(1, 2), T + F(3, 2)}
    for b in B:
        ts |= {b - F(1, 4), b + F(3, 10)}
    for i in range(1, 7):
        ts.add(F(round(float(T) * i / 7 * 1000), 1000))
    pts = []
    for t in sorted(ts):
        pts.append({"t": float(t), "rate": float(rate(segs, t)), "A": float(A(segs, t)),
                    "W": float(W(segs, t)), "L": L(segs, t)})
    anchor = 1700000000000
    buckets = []
    for b0, w in [(-2000, 1000), (0, 1000), (0, 10000), (int(T * 1000) // 2 + 250, 5000), (int(T * 1000) - 500, 3000)]:
        mean = (A(segs, F(b0 + w, 1000)) - A(segs, F(b0, 1000))) / F(w, 1000)
        buckets.append({"b0Ms": anchor + b0, "wMs": w, "anchorMs": anchor, "mean": float(mean)})
    out["cases"].append({"name": name, "startRps": start,
                         "segments": [{"fromRps": a, "toRps": b, "durationSec": d} for a, b, d in segs],
                         "totalSec": int(T), "peak": max([start] + [s[0] for s in stages]),
                         "points": pts, "buckets": buckets})
json.dump(out, open(sys.argv[1], "w"), indent=1)
print(sum(len(c["points"]) for c in out["cases"]), "points")
