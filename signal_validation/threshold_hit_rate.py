"""
Test 2: NS Score Threshold Hit Rate

Question: When NS exceeds a threshold, does the associated ticker actually move?

Method:
  - Identify dates where a narrative's NS score crossed above threshold T.
  - For each crossing, check whether the linked ticker moved > 1% (abs)
    within 1, 2, and 5 trading days.
  - Compare hit rate against a random-date baseline for the same tickers.

Success criteria: Hit rate at T=0.7 should exceed baseline by >= 10pp.

Output: threshold_hit_rate.png
"""

import random
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from .shared import (
    get_db, get_ohlcv, trading_days_after, narrative_ticker_pairs,
    snapshot_date_range, print_header, print_separator, OUT_DIR,
)

THRESHOLDS = [0.5, 0.6, 0.7, 0.8]
WINDOWS = [1, 2, 5]
MOVE_PCT = 0.01  # 1 %


# ── crossing detection ─────────────────────────────────────────────

def _find_crossings(conn, threshold):
    """
    Return [(narrative_id, name, ticker, crossing_date)] where NS went
    from below *threshold* on day N-1 to at-or-above on day N.
    """
    pairs = narrative_ticker_pairs(conn, min_snaps=3, max_pairs=50)
    crossings = []

    for nid, name, ticker, _sc, _ns in pairs:
        c = conn.cursor()
        c.execute("""
            SELECT snapshot_date, ns_score
            FROM narrative_snapshots
            WHERE narrative_id = ? AND ns_score IS NOT NULL
            ORDER BY snapshot_date
        """, (nid,))
        prev = None
        for row in c.fetchall():
            score = row["ns_score"]
            if prev is not None and prev < threshold <= score:
                crossings.append((nid, name, ticker, row["snapshot_date"]))
            prev = score

    return crossings


# ── hit-rate computation ───────────────────────────────────────────

def _hit_rate(crossings, conn, window):
    """
    For each crossing event check whether the ticker moved > MOVE_PCT
    within *window* trading days.  Returns (hits, total, rate).
    """
    min_d, max_d = snapshot_date_range(conn)
    hits = total = 0

    for _nid, _name, ticker, date_str in crossings:
        prices = get_ohlcv(ticker, min_d, max_d)
        if not prices or date_str not in prices:
            continue
        future = trading_days_after(prices, date_str, window)
        if not future:
            continue

        base = prices[date_str]["close"]
        moved = any(
            abs(prices[fd]["close"] - base) / base > MOVE_PCT
            for fd in future
        )
        total += 1
        hits += int(moved)

    return hits, total, (hits / total if total else 0.0)


def _baseline(conn, tickers, n_samples_per_ticker, window):
    """
    Pick random dates for the same tickers and compute the same hit rate.
    """
    min_d, max_d = snapshot_date_range(conn)
    hits = total = 0

    for ticker in set(tickers):
        prices = get_ohlcv(ticker, min_d, max_d)
        if not prices:
            continue
        dates = sorted(prices)
        if len(dates) < window + 5:
            continue

        pool = dates[: -window - 1]          # room for future window
        sample = random.sample(pool, min(n_samples_per_ticker, len(pool)))

        for d in sample:
            future = trading_days_after(prices, d, window)
            if not future:
                continue
            base = prices[d]["close"]
            moved = any(
                abs(prices[fd]["close"] - base) / base > MOVE_PCT
                for fd in future
            )
            total += 1
            hits += int(moved)

    return hits / total if total else 0.0


# ── plotting ───────────────────────────────────────────────────────

def _plot(results):
    n_win = len(WINDOWS)
    fig, axes = plt.subplots(1, n_win, figsize=(5.5 * n_win, 6), sharey=True)
    if n_win == 1:
        axes = [axes]

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    fig.suptitle(f"Test 2: NS Threshold Hit Rate vs Baseline\n(generated {ts})",
                 fontsize=14, fontweight="bold")

    for i, w in enumerate(WINDOWS):
        ax = axes[i]
        labels, hr, bl, ns = [], [], [], []
        for T in THRESHOLDS:
            r = results[T][w]
            labels.append(str(T))
            hr.append(r["hit_rate"] * 100)
            bl.append(r["baseline"] * 100)
            ns.append(r["n"])

        x = np.arange(len(labels))
        width = 0.32
        b1 = ax.bar(x - width / 2, hr, width, label="Hit Rate", color="#4A90D9", alpha=0.85)
        ax.bar(x + width / 2, bl, width, label="Baseline", color="#999999", alpha=0.6)

        for j, (bar, n) in enumerate(zip(b1, ns)):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                    f"n={n}", ha="center", va="bottom", fontsize=8)

        ax.set_xlabel("NS Threshold")
        if i == 0:
            ax.set_ylabel("Hit Rate (%)")
        ax.set_title(f"{w}-Day Window")
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend(fontsize=9)
        ax.grid(axis="y", alpha=0.3)

        # 10pp-above-baseline reference line per threshold
        for j, b in enumerate(bl):
            ax.plot([x[j] - 0.4, x[j] + 0.4], [b + 10, b + 10],
                    color="#E5533C", linestyle="--", linewidth=1, alpha=0.5)

    plt.tight_layout()
    out = OUT_DIR / "threshold_hit_rate.png"
    fig.savefig(str(out), dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\n  -> {out}")


# ── entry point ────────────────────────────────────────────────────

def run():
    """Run Test 2.  Returns {threshold: {window: {hit_rate, baseline, n, lift, pass}}}."""
    conn = get_db()
    random.seed(42)

    print_header("Test 2: NS Score Threshold Hit Rate")
    results: dict = {}

    for T in THRESHOLDS:
        crossings = _find_crossings(conn, T)
        tickers = [c[2] for c in crossings]
        print(f"\n  Threshold T={T}: {len(crossings)} crossing events")

        results[T] = {}
        for w in WINDOWS:
            if not crossings:
                results[T][w] = {"hit_rate": 0, "baseline": 0, "n": 0, "lift": 0, "pass": False}
                continue

            hits, total, rate = _hit_rate(crossings, conn, w)
            bl = _baseline(conn, tickers, max(len(crossings), 20), w)
            lift = rate - bl
            passed = lift >= 0.10
            results[T][w] = {"hit_rate": rate, "baseline": bl, "n": total, "lift": lift, "pass": passed}
            print(f"    {w}d: hit={rate:.1%}  base={bl:.1%}  lift={lift:+.1%}  n={total}  {'PASS' if passed else 'fail'}")

    # summary table
    print_separator()
    print(f"  {'Thresh':>7} {'Win':>5} {'Hit Rate':>10} {'Baseline':>10} {'Lift':>8} {'n':>5} {'Pass':>6}")
    for T in THRESHOLDS:
        for w in WINDOWS:
            r = results[T][w]
            tag = "PASS" if r["pass"] else "fail"
            print(f"  {T:>7.1f} {w:>4}d {r['hit_rate']:>10.1%} {r['baseline']:>10.1%} "
                  f"{r['lift']:>+7.1%} {r['n']:>5} {tag:>6}")

    # success flag: T=0.7 at any window passes
    success = any(results[0.7][w]["pass"] for w in WINDOWS)
    status = "PASS" if success else "FAIL"
    print(f"\n  Test 2 overall: {status}  (T=0.7 lift >= 10pp at any window)")

    _plot(results)
    conn.close()
    return {"results": results, "pass": success}


if __name__ == "__main__":
    run()
