"""
Test 5: Source Escalation as Leading Indicator

Question: Does institutional pickup (tier 1/2 sources covering a narrative)
          precede larger price moves?

Method:
  - Identify narratives where source_institutional_pickup = 1.
  - From document_evidence, find the earliest tier 1/2 publication date
    per narrative  -- that is the "pickup date".
  - Compare linked-ticker performance in the 5 trading days before vs.
    5 trading days after institutional pickup.
  - Control group: narratives that never reached tier 1/2 but had
    similar NS scores (within 0.1).

Success criteria: Post-pickup avg |price change| exceeds pre-pickup
by >= 50 % AND exceeds the control group.

Output: source_escalation_impact.png
"""

import json
from datetime import datetime
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from .shared import (
    get_db, get_ohlcv, trading_days_before, trading_days_after,
    snapshot_date_range, print_header, print_separator, OUT_DIR,
)

# Tier 1/2 domain fragments (mirrors source_tiers.py logic)
TIER_1_2_FRAGMENTS = [
    # Tier 1 — wire services
    "reuters", "apnews", "ap.org", "bloomberg", "wsj.com", "ft.com",
    # Tier 2 — major business press
    "cnbc.com", "marketwatch", "barrons", "bbc.co", "bbc.com",
    "nytimes", "washingtonpost", "theguardian", "economist.com",
    "fortune.com", "forbes.com",
]

WINDOW = 5  # trading days before/after


def _is_tier_1_2(domain: str) -> bool:
    if not domain:
        return False
    dl = domain.lower()
    return any(frag in dl for frag in TIER_1_2_FRAGMENTS)


def _parse_tickers(json_str):
    if not json_str:
        return []
    try:
        assets = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return []
    out = []
    for a in assets:
        t = a.get("ticker", "") if isinstance(a, dict) else (a if isinstance(a, str) else "")
        if t and not t.startswith("TOPIC:"):
            out.append(t)
    return out


# ── data collection ────────────────────────────────────────────────

def _pickup_narratives(conn):
    """
    Return [(narrative_id, name, ns_score, tickers, pickup_date)] for
    narratives with institutional pickup.  pickup_date comes from the
    earliest tier 1/2 document in document_evidence.
    """
    c = conn.cursor()
    c.execute("""
        SELECT narrative_id, name, ns_score, linked_assets
        FROM narratives
        WHERE source_institutional_pickup = 1
          AND suppressed = 0
          AND linked_assets IS NOT NULL AND linked_assets != '[]'
    """)
    candidates = []
    for row in c.fetchall():
        tickers = _parse_tickers(row["linked_assets"])
        if tickers:
            candidates.append((row["narrative_id"], row["name"], row["ns_score"], tickers))

    results = []
    for nid, name, ns, tickers in candidates:
        c.execute("""
            SELECT source_domain, published_at
            FROM document_evidence
            WHERE narrative_id = ?
              AND published_at IS NOT NULL
            ORDER BY published_at ASC
        """, (nid,))
        pickup_date = None
        for doc in c.fetchall():
            if _is_tier_1_2(doc["source_domain"]):
                pickup_date = doc["published_at"][:10]  # YYYY-MM-DD
                break
        if pickup_date:
            results.append((nid, name, ns, tickers, pickup_date))

    return results


def _control_narratives(conn, target_ns_scores):
    """
    Return [(narrative_id, name, ns_score, tickers)] for narratives
    that never reached tier 1/2 but have NS scores within 0.1 of at
    least one target score.
    """
    c = conn.cursor()
    c.execute("""
        SELECT narrative_id, name, ns_score, linked_assets
        FROM narratives
        WHERE (source_institutional_pickup = 0 OR source_institutional_pickup IS NULL)
          AND suppressed = 0
          AND linked_assets IS NOT NULL AND linked_assets != '[]'
    """)
    results = []
    for row in c.fetchall():
        ns = row["ns_score"] or 0.0
        if any(abs(ns - t) <= 0.1 for t in target_ns_scores):
            tickers = _parse_tickers(row["linked_assets"])
            if tickers:
                results.append((row["narrative_id"], row["name"], ns, tickers))
    return results


def _avg_abs_move(tickers, center_date, conn, direction="after"):
    """
    Average absolute price change over WINDOW trading days before or
    after center_date, across all tickers.
    """
    min_d, max_d = snapshot_date_range(conn)
    moves = []
    for ticker in tickers:
        ohlcv = get_ohlcv(ticker, min_d, max_d)
        if not ohlcv:
            continue

        if direction == "after":
            days = trading_days_after(ohlcv, center_date, WINDOW)
        else:
            days = trading_days_before(ohlcv, center_date, WINDOW)

        if not days:
            continue

        # compute daily returns within the window
        all_dates = sorted(ohlcv)
        for d in days:
            idx = all_dates.index(d) if d in all_dates else -1
            if idx > 0:
                prev = ohlcv[all_dates[idx - 1]]["close"]
                cur = ohlcv[d]["close"]
                if prev > 0:
                    moves.append(abs((cur - prev) / prev))

    return float(np.mean(moves)) if moves else None


def _control_avg_move(control, conn):
    """Average absolute daily price change for control narratives (using
    most recent snapshot date as anchor, measuring the next WINDOW days)."""
    min_d, max_d = snapshot_date_range(conn)
    moves = []
    for nid, _name, _ns, tickers in control:
        # use latest snapshot date as anchor
        c = conn.cursor()
        c.execute("""
            SELECT MAX(snapshot_date) as d FROM narrative_snapshots
            WHERE narrative_id = ?
        """, (nid,))
        row = c.fetchone()
        anchor = row["d"] if row and row["d"] else None
        if not anchor:
            continue

        for ticker in tickers:
            ohlcv = get_ohlcv(ticker, min_d, max_d)
            if not ohlcv:
                continue
            days = trading_days_after(ohlcv, anchor, WINDOW)
            all_dates = sorted(ohlcv)
            for d in days:
                idx = all_dates.index(d) if d in all_dates else -1
                if idx > 0:
                    prev = ohlcv[all_dates[idx - 1]]["close"]
                    cur = ohlcv[d]["close"]
                    if prev > 0:
                        moves.append(abs((cur - prev) / prev))

    return float(np.mean(moves)) if moves else None


# ── plotting ───────────────────────────────────────────────────────

def _plot(pre, post, control_avg, n_pickup, n_control):
    fig, ax = plt.subplots(figsize=(8, 6))
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    fig.suptitle(f"Test 5: Source Escalation (Institutional Pickup) Impact\n(generated {ts})",
                 fontsize=14, fontweight="bold")

    labels = []
    values = []
    colors = []

    if pre is not None:
        labels.append(f"Pre-Pickup\n(5d before)\nn_narr={n_pickup}")
        values.append(pre * 100)
        colors.append("#4A90D9")
    if post is not None:
        labels.append(f"Post-Pickup\n(5d after)\nn_narr={n_pickup}")
        values.append(post * 100)
        colors.append("#E5533C")
    if control_avg is not None:
        labels.append(f"Control Group\n(no pickup)\nn_narr={n_control}")
        values.append(control_avg * 100)
        colors.append("#999999")

    if not values:
        print("  [SKIP] No data to plot")
        plt.close()
        return

    x = np.arange(len(labels))
    bars = ax.bar(x, values, color=colors, alpha=0.85, width=0.5)

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                f"{val:.3f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel("Avg |Daily Price Change| (%)")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    out = OUT_DIR / "source_escalation_impact.png"
    fig.savefig(str(out), dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\n  -> {out}")


# ── entry point ────────────────────────────────────────────────────

def run():
    """Run Test 5.  Returns pre/post/control averages + pass/fail."""
    conn = get_db()

    print_header("Test 5: Source Escalation as Leading Indicator")

    pickups = _pickup_narratives(conn)
    print(f"\n  {len(pickups)} narratives with institutional pickup + ticker + pickup date")

    if not pickups:
        print("  [INSUFFICIENT DATA] No narratives with institutional pickup found.")
        conn.close()
        return {"pre": None, "post": None, "control": None, "pass": False, "reason": "insufficient_data"}

    for _nid, name, _ns, tickers, pdate in pickups:
        print(f"    {name[:50]:<52} tickers={tickers}  pickup={pdate}")

    # compute pre/post
    pre_moves = []
    post_moves = []
    for nid, name, ns, tickers, pdate in pickups:
        pre = _avg_abs_move(tickers, pdate, conn, direction="before")
        post = _avg_abs_move(tickers, pdate, conn, direction="after")
        if pre is not None:
            pre_moves.append(pre)
        if post is not None:
            post_moves.append(post)

    pre_avg = float(np.mean(pre_moves)) if pre_moves else None
    post_avg = float(np.mean(post_moves)) if post_moves else None

    # control group
    target_scores = [ns for _, _, ns, _, _ in pickups if ns is not None]
    control = _control_narratives(conn, target_scores)
    control_avg = _control_avg_move(control, conn)

    print(f"\n  Control group: {len(control)} narratives (no institutional pickup, similar NS)")

    # summary
    print_separator()
    print(f"  {'Group':<25} {'Avg |Daily Change|':>20} {'n narratives':>14}")
    if pre_avg is not None:
        print(f"  {'Pre-Pickup (5d before)':<25} {pre_avg*100:>19.4f}% {len(pre_moves):>14}")
    if post_avg is not None:
        print(f"  {'Post-Pickup (5d after)':<25} {post_avg*100:>19.4f}% {len(post_moves):>14}")
    if control_avg is not None:
        print(f"  {'Control (no pickup)':<25} {control_avg*100:>19.4f}% {len(control):>14}")

    # pass criteria
    success = False
    if pre_avg and post_avg and pre_avg > 0:
        lift = (post_avg - pre_avg) / pre_avg
        print(f"\n  Post vs Pre lift: {lift:+.1%}  (need >= +50%)")
        if lift >= 0.50:
            if control_avg is None or post_avg > control_avg:
                success = True
                print("  Post-pickup also exceeds control group.")
            else:
                print(f"  Post-pickup ({post_avg:.4f}) does NOT exceed control ({control_avg:.4f}).")

    status = "PASS" if success else "FAIL"
    print(f"\n  Test 5 overall: {status}")

    _plot(pre_avg, post_avg, control_avg, len(pickups), len(control))
    conn.close()
    return {"pre": pre_avg, "post": post_avg, "control": control_avg, "pass": success}


if __name__ == "__main__":
    run()
