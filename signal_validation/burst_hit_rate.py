"""
Test 7: Burst Velocity Hit Rate

Question: When a burst is detected, does the associated ticker see
          abnormal activity within 48 hours (2 trading days)?

Method:
  - Identify all snapshots where burst_ratio >= 3.0 (the alert threshold).
  - For each burst event, pull the linked ticker's volume and price
    for the next 2 trading days.
  - Compute:
      (a) volume_hit: volume > 1.5x the 20-day average
      (b) price_hit:  price moved > 2% absolute
  - Combined hit rate = events where (a) OR (b) triggered / total.

Success criteria: Combined hit rate > 40%.
If below 30%, burst detection is too noisy.

Output: burst_hit_rate.png
"""

import json
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from .shared import (
    get_db, get_ohlcv, trading_days_after, avg_volume_window,
    snapshot_date_range, print_header, print_separator, OUT_DIR,
)

BURST_THRESHOLD = 3.0
VOLUME_MULTIPLE = 1.5
PRICE_MOVE_PCT = 0.02   # 2 %
WINDOW_DAYS = 2


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

def _find_burst_events(conn):
    """
    Return [(narrative_id, name, ticker, snapshot_date, burst_ratio)]
    for snapshots where burst_ratio >= BURST_THRESHOLD.
    """
    c = conn.cursor()
    c.execute("""
        SELECT s.narrative_id, s.snapshot_date, s.burst_ratio, s.linked_assets,
               n.name, n.linked_assets AS n_assets
        FROM narrative_snapshots s
        JOIN narratives n ON s.narrative_id = n.narrative_id
        WHERE s.burst_ratio >= ?
        ORDER BY s.snapshot_date
    """, (BURST_THRESHOLD,))

    events = []
    for row in c.fetchall():
        # prefer snapshot linked_assets, fall back to narrative
        tickers = _parse_tickers(row["linked_assets"]) or _parse_tickers(row["n_assets"])
        for ticker in tickers:
            events.append((
                row["narrative_id"],
                row["name"],
                ticker,
                row["snapshot_date"],
                row["burst_ratio"],
            ))

    return events


def _evaluate_event(ticker, date, conn):
    """
    Check volume and price criteria for a burst event.
    Returns (volume_hit, price_hit, vol_ratio, price_pct) or None.
    """
    min_d, max_d = snapshot_date_range(conn)
    ohlcv = get_ohlcv(ticker, min_d, max_d)
    if not ohlcv:
        return None

    # find anchor date (nearest trading day <= date)
    if date not in ohlcv:
        prior = [d for d in sorted(ohlcv) if d <= date]
        if not prior:
            return None
        date = prior[-1]

    future = trading_days_after(ohlcv, date, WINDOW_DAYS)
    if not future:
        return None

    base_price = ohlcv[date]["close"]
    avg_vol = avg_volume_window(ohlcv, date, 20)

    # check criteria across the window
    vol_hit = False
    price_hit = False
    max_vol_ratio = 0.0
    max_price_pct = 0.0

    for fd in future:
        day_vol = ohlcv[fd]["volume"]
        day_price = ohlcv[fd]["close"]

        if avg_vol > 0:
            ratio = day_vol / avg_vol
            max_vol_ratio = max(max_vol_ratio, ratio)
            if ratio > VOLUME_MULTIPLE:
                vol_hit = True

        pct = abs(day_price - base_price) / base_price
        max_price_pct = max(max_price_pct, pct)
        if pct > PRICE_MOVE_PCT:
            price_hit = True

    return vol_hit, price_hit, max_vol_ratio, max_price_pct


# ── plotting ───────────────────────────────────────────────────────

def _plot(vol_rate, price_rate, combined_rate, n_events, event_details):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    fig.suptitle(f"Test 7: Burst Velocity Hit Rate\n(generated {ts})",
                 fontsize=14, fontweight="bold")

    # ── left: hit rate bars ──
    categories = ["Volume\n(>1.5x 20d avg)", "Price\n(>2% move)", "Combined\n(either)"]
    rates = [vol_rate * 100, price_rate * 100, combined_rate * 100]
    colors = ["#4A90D9", "#E5533C", "#7ED321"]

    bars = ax1.bar(categories, rates, color=colors, alpha=0.85, width=0.5)
    for bar, rate in zip(bars, rates):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                 f"{rate:.1f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax1.axhline(y=40, color="#7ED321", linestyle="--", linewidth=1.5, label="40% target")
    ax1.axhline(y=30, color="#F5A623", linestyle=":", linewidth=1.5, label="30% noise floor")
    ax1.set_ylabel("Hit Rate (%)")
    ax1.set_title(f"Hit Rates (n={n_events} burst events)")
    ax1.set_ylim(0, max(max(rates) + 15, 50))
    ax1.legend(fontsize=9)
    ax1.grid(axis="y", alpha=0.3)

    # ── right: scatter of burst_ratio vs max price move ──
    if event_details:
        ratios = [e["burst_ratio"] for e in event_details]
        price_pcts = [e["max_price_pct"] * 100 for e in event_details]
        combined_hits = [e["vol_hit"] or e["price_hit"] for e in event_details]
        colors_sc = ["#7ED321" if h else "#E5533C" for h in combined_hits]

        ax2.scatter(ratios, price_pcts, c=colors_sc, alpha=0.7, s=60, edgecolors="white", linewidth=0.5)
        ax2.axhline(y=2, color="#E5533C", linestyle="--", linewidth=1, alpha=0.5)
        ax2.set_xlabel("Burst Ratio")
        ax2.set_ylabel("Max |Price Change| in 48h (%)")
        ax2.set_title("Burst Ratio vs. Subsequent Price Move")
        ax2.grid(alpha=0.3)
        # legend
        from matplotlib.lines import Line2D
        legend_elements = [
            Line2D([0], [0], marker="o", color="w", markerfacecolor="#7ED321", markersize=8, label="Hit"),
            Line2D([0], [0], marker="o", color="w", markerfacecolor="#E5533C", markersize=8, label="Miss"),
        ]
        ax2.legend(handles=legend_elements, fontsize=9)

    plt.tight_layout()
    out = OUT_DIR / "burst_hit_rate.png"
    fig.savefig(str(out), dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\n  -> {out}")


# ── entry point ────────────────────────────────────────────────────

def run():
    """Run Test 7.  Returns hit rate stats + pass/fail."""
    conn = get_db()

    print_header("Test 7: Burst Velocity Hit Rate")

    events = _find_burst_events(conn)
    print(f"\n  {len(events)} burst events (burst_ratio >= {BURST_THRESHOLD})")

    if not events:
        print("  [INSUFFICIENT DATA] No burst events found in snapshots.")
        conn.close()
        return {"vol_rate": 0, "price_rate": 0, "combined_rate": 0, "n": 0,
                "pass": False, "reason": "insufficient_data"}

    # deduplicate: same (ticker, date) can appear from multiple narratives
    seen = set()
    unique_events = []
    for nid, name, ticker, date, br in events:
        key = (ticker, date)
        if key not in seen:
            unique_events.append((nid, name, ticker, date, br))
            seen.add(key)

    print(f"  {len(unique_events)} unique (ticker, date) burst events")

    vol_hits = price_hits = combined_hits = 0
    evaluated = 0
    event_details = []

    for nid, name, ticker, date, br in unique_events:
        result = _evaluate_event(ticker, date, conn)
        if result is None:
            continue

        vol_hit, price_hit, vol_ratio, price_pct = result
        evaluated += 1
        if vol_hit:
            vol_hits += 1
        if price_hit:
            price_hits += 1
        if vol_hit or price_hit:
            combined_hits += 1

        event_details.append({
            "narrative": name, "ticker": ticker, "date": date,
            "burst_ratio": br, "vol_hit": vol_hit, "price_hit": price_hit,
            "vol_ratio": vol_ratio, "max_price_pct": price_pct,
        })

    vol_rate = vol_hits / evaluated if evaluated else 0
    price_rate = price_hits / evaluated if evaluated else 0
    combined_rate = combined_hits / evaluated if evaluated else 0

    # detail table
    print_separator()
    print(f"  {'Narrative':<40} {'Ticker':<7} {'Date':<12} {'BR':>5} "
          f"{'VolR':>6} {'|Chg%|':>7} {'Vol':>4} {'Prc':>4}")
    for e in event_details:
        v_tag = "HIT" if e["vol_hit"] else " - "
        p_tag = "HIT" if e["price_hit"] else " - "
        print(f"  {e['narrative'][:39]:<40} {e['ticker']:<7} {e['date']:<12} "
              f"{e['burst_ratio']:>5.1f} {e['vol_ratio']:>5.1f}x "
              f"{e['max_price_pct']*100:>6.2f}% {v_tag:>4} {p_tag:>4}")

    # summary
    print_separator()
    print(f"  Volume hit rate:   {vol_rate:.1%}  ({vol_hits}/{evaluated})")
    print(f"  Price hit rate:    {price_rate:.1%}  ({price_hits}/{evaluated})")
    print(f"  Combined hit rate: {combined_rate:.1%}  ({combined_hits}/{evaluated})")

    success = combined_rate > 0.40
    noisy = combined_rate < 0.30
    if success:
        status = "PASS"
    elif noisy:
        status = "FAIL (too noisy, needs threshold tuning)"
    else:
        status = "FAIL (marginal, between 30-40%)"

    print(f"\n  Test 7 overall: {status}")

    _plot(vol_rate, price_rate, combined_rate, evaluated, event_details)
    conn.close()
    return {"vol_rate": vol_rate, "price_rate": price_rate,
            "combined_rate": combined_rate, "n": evaluated, "pass": success}


if __name__ == "__main__":
    run()
