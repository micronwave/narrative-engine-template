"""
Early Signal Validation — visual + numerical check.

Plots narrative velocity vs. ticker price for top narrative/ticker pairs.
Outputs a single PNG with subplots + prints a correlation summary table.

Usage:
    python validate_signal.py
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import yfinance as yf
import numpy as np

DB_PATH = Path(__file__).parent / "data" / "narrative_engine.db"
OUT_PNG = Path(__file__).parent / "signal_validation.png"

# ── Gather narrative/ticker pairs ────────────────────────────────────

def get_pairs(conn):
    """Return list of (narrative_id, name, ticker, snap_days, doc_count)."""
    c = conn.cursor()
    c.execute("""
        SELECT n.narrative_id, n.name, n.linked_assets, n.document_count,
               (SELECT COUNT(*) FROM narrative_snapshots s
                WHERE s.narrative_id = n.narrative_id) AS snap_days
        FROM narratives n
        WHERE n.suppressed = 0
          AND n.linked_assets IS NOT NULL AND n.linked_assets != '[]'
        ORDER BY snap_days DESC, n.document_count DESC
    """)
    pairs = []
    seen_tickers = set()
    for row in c.fetchall():
        assets = json.loads(row[2])
        for a in assets:
            ticker = a.get("ticker", "") if isinstance(a, dict) else (a if isinstance(a, str) else "")
            if not ticker or ticker.startswith("TOPIC:"):
                continue
            # prefer unique tickers for diversity, but allow duplicates if few pairs
            key = (row[0], ticker)
            if key not in seen_tickers:
                pairs.append((row[0], row[1], ticker, row[4], row[3]))
                seen_tickers.add(key)
            if len(pairs) >= 8:
                break
        if len(pairs) >= 8:
            break
    return pairs


# ── Load velocity snapshots ──────────────────────────────────────────

def get_velocity_series(conn, narrative_id):
    """Return dict {date_str: velocity}."""
    c = conn.cursor()
    c.execute("""
        SELECT snapshot_date, velocity, ns_score, doc_count
        FROM narrative_snapshots
        WHERE narrative_id = ?
        ORDER BY snapshot_date
    """, (narrative_id,))
    data = {}
    for row in c.fetchall():
        data[row[0]] = {
            "velocity": row[1] or 0.0,
            "ns_score": row[2] or 0.0,
            "doc_count": row[3] or 0,
        }
    return data


# ── Load price data ──────────────────────────────────────────────────

def get_price_series(ticker, start_date, end_date):
    """Fetch daily close prices from yfinance. Returns dict {date_str: close}."""
    # pad a few days on each side
    start = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=3)
    end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=3)
    try:
        df = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                         end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if df.empty:
            return {}
        result = {}
        for idx, row in df.iterrows():
            date_str = idx.strftime("%Y-%m-%d")
            close_val = row["Close"]
            # handle potential multi-index from yfinance
            if hasattr(close_val, "item"):
                close_val = close_val.item()
            result[date_str] = float(close_val)
        return result
    except Exception as e:
        print(f"  [WARN] yfinance failed for {ticker}: {e}")
        return {}


# ── Compute simple correlation ───────────────────────────────────────

def compute_correlation(velocity_series, price_series):
    """
    Align velocity and price by date.
    Returns (dates, velocities, prices, pearson_r, n).
    Also computes velocity vs. next-day price change.
    """
    common_dates = sorted(set(velocity_series.keys()) & set(price_series.keys()))
    if len(common_dates) < 3:
        return common_dates, [], [], None, 0, None

    dates = common_dates
    vels = [velocity_series[d]["velocity"] for d in dates]
    prices = [price_series[d] for d in dates]

    # Same-day correlation: velocity vs. price
    r_same = None
    if np.std(vels) > 0 and np.std(prices) > 0:
        r_same = float(np.corrcoef(vels, prices)[0, 1])

    # Lead correlation: velocity today vs. price change tomorrow
    r_lead = None
    if len(dates) >= 4:
        all_dates_sorted = sorted(price_series.keys())
        price_changes = []
        vel_aligned = []
        for d in dates:
            idx = all_dates_sorted.index(d) if d in all_dates_sorted else -1
            if idx >= 0 and idx + 1 < len(all_dates_sorted):
                next_d = all_dates_sorted[idx + 1]
                pct_change = (price_series[next_d] - price_series[d]) / price_series[d]
                price_changes.append(pct_change)
                vel_aligned.append(velocity_series[d]["velocity"])
        if len(price_changes) >= 3 and np.std(vel_aligned) > 0 and np.std(price_changes) > 0:
            r_lead = float(np.corrcoef(vel_aligned, price_changes)[0, 1])

    return dates, vels, prices, r_same, len(dates), r_lead


# ── Plot ─────────────────────────────────────────────────────────────

def plot_all(results):
    n = len(results)
    fig, axes = plt.subplots(n, 1, figsize=(14, 4 * n), sharex=False)
    if n == 1:
        axes = [axes]

    fig.suptitle("Early Signal Validation: Narrative Velocity vs. Ticker Price",
                 fontsize=16, fontweight="bold", y=1.0)

    for i, res in enumerate(results):
        ax1 = axes[i]
        name = res["name"][:50]
        ticker = res["ticker"]
        r_same = res["r_same"]
        r_lead = res["r_lead"]
        n_obs = res["n"]

        dates = [datetime.strptime(d, "%Y-%m-%d") for d in res["dates"]]
        vels = res["velocities"]
        prices = res["prices"]

        if not dates:
            ax1.text(0.5, 0.5, f"{name} / {ticker}\nNo overlapping data",
                     ha="center", va="center", transform=ax1.transAxes, fontsize=12)
            continue

        # Velocity (left axis)
        color_vel = "#4A90D9"
        ax1.set_ylabel("Velocity", color=color_vel, fontsize=10)
        ax1.plot(dates, vels, color=color_vel, marker="o", linewidth=2, markersize=6, label="Velocity")
        ax1.tick_params(axis="y", labelcolor=color_vel)
        ax1.fill_between(dates, vels, alpha=0.15, color=color_vel)

        # Price (right axis)
        ax2 = ax1.twinx()
        color_price = "#E5533C"
        ax2.set_ylabel(f"{ticker} Close ($)", color=color_price, fontsize=10)
        ax2.plot(dates, prices, color=color_price, marker="s", linewidth=2, markersize=6, label=f"{ticker}")
        ax2.tick_params(axis="y", labelcolor=color_price)

        # Title with correlation info
        r_same_str = f"r={r_same:+.3f}" if r_same is not None else "r=N/A"
        r_lead_str = f"r_lead={r_lead:+.3f}" if r_lead is not None else "r_lead=N/A"
        ax1.set_title(f"{name}  |  {ticker}  |  {r_same_str}  |  {r_lead_str}  |  n={n_obs}",
                       fontsize=11, fontweight="bold", pad=10)

        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
        ax1.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(str(OUT_PNG), dpi=150, bbox_inches="tight")
    print(f"\nChart saved to: {OUT_PNG}")
    plt.close()


# ── Summary table ────────────────────────────────────────────────────

def print_summary(results):
    print("\n" + "=" * 100)
    print("EARLY SIGNAL VALIDATION SUMMARY")
    print("=" * 100)
    print(f"{'Narrative':<45} {'Ticker':<8} {'n':>3} {'r_same':>8} {'r_lead':>8} {'Direction':>10}")
    print("-" * 100)

    for res in results:
        name = res["name"][:44]
        ticker = res["ticker"]
        n_obs = res["n"]
        r_same = res["r_same"]
        r_lead = res["r_lead"]

        r_same_str = f"{r_same:+.3f}" if r_same is not None else "  N/A"
        r_lead_str = f"{r_lead:+.3f}" if r_lead is not None else "  N/A"

        if r_lead is not None:
            if r_lead > 0.3:
                direction = "PROMISING"
            elif r_lead < -0.3:
                direction = "INVERSE"
            else:
                direction = "WEAK"
        else:
            direction = "NO DATA"

        print(f"{name:<45} {ticker:<8} {n_obs:>3} {r_same_str:>8} {r_lead_str:>8} {direction:>10}")

    print("-" * 100)
    print("r_same  = Pearson correlation between velocity and price (same day)")
    print("r_lead  = Pearson correlation between today's velocity and tomorrow's price change")
    print("         Positive r_lead = velocity rises before price rises (predictive signal)")
    print(f"\nWARNING: n < 30 — these are DIRECTIONAL INDICATORS only, not statistically significant.")
    print(f"         Re-run after April 28 for formal Phase 0 validation.\n")


# ── Main ─────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    pairs = get_pairs(conn)

    print(f"Found {len(pairs)} narrative/ticker pairs to validate.\n")

    results = []
    for narrative_id, name, ticker, snap_days, doc_count in pairs:
        print(f"Processing: {name[:50]} / {ticker} ({snap_days} snapshots, {doc_count} docs)")

        vel_data = get_velocity_series(conn, narrative_id)
        if not vel_data:
            print("  [SKIP] No velocity snapshots")
            continue

        date_range = sorted(vel_data.keys())
        price_data = get_price_series(ticker, date_range[0], date_range[-1])
        if not price_data:
            print("  [SKIP] No price data from yfinance")
            continue

        dates, vels, prices, r_same, n, r_lead = compute_correlation(vel_data, price_data)
        print(f"  Aligned {n} data points | r_same={r_same} | r_lead={r_lead}")

        results.append({
            "narrative_id": narrative_id,
            "name": name,
            "ticker": ticker,
            "dates": dates,
            "velocities": vels,
            "prices": prices,
            "r_same": r_same,
            "r_lead": r_lead,
            "n": n,
        })

    conn.close()

    if not results:
        print("No valid pairs found. Ensure pipeline has run and narratives have linked tickers.")
        return

    print_summary(results)
    plot_all(results)


if __name__ == "__main__":
    main()
