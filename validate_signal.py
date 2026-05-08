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

def plot_all(results, output_path=None):
    if output_path is None:
        output_path = OUT_PNG
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
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    print(f"\nChart saved to: {output_path}")
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


###############################################################################
# Mid-Point Validation (Phase 0 Rerun)
# - Compares old undirected ns_score vs new directional impact_score
# - Computes hit rates and correlation analysis at multiple lead times
# - Appends results to BUILD_LOG.md
# - Generates PNG chart for top performers
###############################################################################



def _select_validation_pairs(conn, max_pairs=15):
    """
    Select 10-15 completed narratives (Mature/Declining) with real tickers
    from impact_scores table.  Returns list of dicts:
    {narrative_id, name, ticker, direction, impact_score, ns_score, stage}.
    """
    c = conn.cursor()

    # Check if impact_scores table exists
    table_check = c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='impact_scores'"
    ).fetchone()
    if not table_check:
        print("[WARN] impact_scores table does not exist — run pipeline first.")
        return []

    # Get Mature/Declining narratives that have impact scores
    c.execute("""
        SELECT DISTINCT n.narrative_id, n.name, n.stage, n.ns_score,
               i.ticker, i.direction, i.impact_score, i.confidence
        FROM narratives n
        JOIN impact_scores i ON n.narrative_id = i.narrative_id
        WHERE n.stage IN ('Mature', 'Declining')
          AND i.ticker NOT LIKE 'TOPIC:%'
          AND n.suppressed = 0
        ORDER BY i.impact_score DESC
        LIMIT ?
    """, (max_pairs * 2,))  # fetch extra, dedupe below

    rows = c.fetchall()
    if not rows:
        # Fall back: any narratives with impact scores (ignore stage)
        c.execute("""
            SELECT DISTINCT n.narrative_id, n.name, n.stage, n.ns_score,
                   i.ticker, i.direction, i.impact_score, i.confidence
            FROM narratives n
            JOIN impact_scores i ON n.narrative_id = i.narrative_id
            WHERE i.ticker NOT LIKE 'TOPIC:%'
              AND n.suppressed = 0
            ORDER BY i.impact_score DESC
            LIMIT ?
        """, (max_pairs * 2,))
        rows = c.fetchall()

    # Deduplicate by (narrative_id, ticker) — take highest impact_score
    seen = set()
    pairs = []
    for row in rows:
        key = (row[0], row[4])
        if key in seen:
            continue
        seen.add(key)
        pairs.append({
            "narrative_id": row[0],
            "name": row[1],
            "stage": row[2],
            "ns_score": row[3] or 0.0,
            "ticker": row[4],
            "direction": row[5] or "neutral",
            "impact_score": row[6] or 0.0,
            "confidence": row[7] or 0.0,
        })
        if len(pairs) >= max_pairs:
            break

    return pairs


def _get_peak_velocity_date(conn, narrative_id):
    """Find the date of peak velocity from snapshots."""
    c = conn.cursor()
    c.execute("""
        SELECT snapshot_date, velocity FROM narrative_snapshots
        WHERE narrative_id = ?
        ORDER BY velocity DESC LIMIT 1
    """, (narrative_id,))
    row = c.fetchone()
    if row:
        return row[0]
    return None


def _check_price_movement(ticker, peak_date_str, direction, threshold_pct=2.0, window_days=7):
    """
    Check price movement around peak_date_str. Single yfinance fetch.

    Returns dict:
        data_available: bool
        moved_directional: bool  — moved >threshold in predicted direction
        moved_either: bool       — moved >threshold in either direction
        max_directional: float   — max favorable move for predicted direction
        max_either: float        — max absolute move in either direction
    """
    empty = {"data_available": False, "moved_directional": False, "moved_either": False,
             "max_directional": 0.0, "max_either": 0.0}
    try:
        peak_dt = datetime.strptime(peak_date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return empty

    start = peak_dt - timedelta(days=3)
    end = peak_dt + timedelta(days=window_days + 3)

    try:
        df = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                         end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if df.empty or len(df) < 2:
            return empty
    except Exception:
        return empty

    # Parse prices
    prices = {}
    for idx, row in df.iterrows():
        d = idx.strftime("%Y-%m-%d")
        close_val = row["Close"]
        if hasattr(close_val, "item"):
            close_val = close_val.item()
        prices[d] = float(close_val)

    peak_key = peak_date_str[:10]
    if peak_key not in prices:
        sorted_dates = sorted(prices.keys())
        closest = min(sorted_dates, key=lambda d: abs(
            (datetime.strptime(d, "%Y-%m-%d") - peak_dt).days
        ))
        if abs((datetime.strptime(closest, "%Y-%m-%d") - peak_dt).days) > 3:
            return empty
        peak_key = closest

    base_price = prices[peak_key]
    sorted_dates = sorted(prices.keys())
    peak_idx = sorted_dates.index(peak_key)

    # Compute max moves in both directions from a single pass
    max_bull = 0.0   # max upward move (positive pct)
    max_bear = 0.0   # max downward move (as positive magnitude)
    for d in sorted_dates[peak_idx:peak_idx + window_days + 1]:
        pct = (prices[d] - base_price) / base_price * 100
        max_bull = max(max_bull, pct)
        max_bear = max(max_bear, -pct)

    max_either = max(max_bull, max_bear)
    if direction == "bullish":
        max_directional = max_bull
    elif direction == "bearish":
        max_directional = max_bear
    else:
        max_directional = 0.0  # neutral has no predicted direction

    return {
        "data_available": True,
        "moved_directional": max_directional > threshold_pct,
        "moved_either": max_either > threshold_pct,
        "max_directional": max_directional,
        "max_either": max_either,
    }


def _run_correlation_at_lags(conn, narrative_id, ticker, lead_days_list=(0, 1, 2, 3, 5, 7)):
    """
    Run correlation analysis at multiple lead times.
    Returns list of {lead_days, r, p_value, n} dicts.
    """
    # Get velocity snapshots
    c = conn.cursor()
    c.execute("""
        SELECT snapshot_date, velocity, ns_score
        FROM narrative_snapshots
        WHERE narrative_id = ?
        ORDER BY snapshot_date
    """, (narrative_id,))
    snap_rows = c.fetchall()
    if not snap_rows or len(snap_rows) < 3:
        return []

    vel_history = [{"date": r[0], "velocity": r[1] or 0.0} for r in snap_rows]
    date_range = sorted(r[0] for r in snap_rows)

    # Get price data
    try:
        start_dt = datetime.strptime(date_range[0][:10], "%Y-%m-%d") - timedelta(days=3)
        end_dt = datetime.strptime(date_range[-1][:10], "%Y-%m-%d") + timedelta(days=10)
        df = yf.download(ticker, start=start_dt.strftime("%Y-%m-%d"),
                         end=end_dt.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if df.empty:
            return []
    except Exception:
        return []

    price_history = []
    prev_close = None
    for idx, row in df.iterrows():
        close_val = row["Close"]
        if hasattr(close_val, "item"):
            close_val = close_val.item()
        close = float(close_val)
        change_pct = ((close - prev_close) / prev_close * 100) if prev_close else 0.0
        price_history.append({
            "date": idx.strftime("%Y-%m-%d"),
            "close": close,
            "change_pct": change_pct,
        })
        prev_close = close

    # Import correlation function
    try:
        from api.correlation_service import compute_velocity_price_correlation
    except ImportError:
        return []

    results = []
    for ld in lead_days_list:
        corr = compute_velocity_price_correlation(vel_history, price_history, lead_days=ld)
        results.append({
            "lead_days": ld,
            "r": corr.get("correlation", 0.0),
            "p_value": corr.get("p_value", 1.0),
            "n": corr.get("n_observations", 0),
            "significant": corr.get("is_significant", False),
        })

    return results


def run_validation():
    """
    Mid-Point Validation: compare old undirected ns_score vs new directional impact_score.

    Outputs summary to stdout and appends results to BUILD_LOG.md.
    Generates PNG chart for top performers.
    """
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    print("=" * 100)
    print("MID-POINT VALIDATION — Signal Pipeline Phase 0 Rerun")
    print("=" * 100)

    # Step 1: Select validation pairs
    pairs = _select_validation_pairs(conn)
    if not pairs:
        msg = "No validation pairs found. Ensure pipeline has run with new signal pipeline."
        print(msg)
        conn.close()
        return {"error": msg, "pairs": 0}

    print(f"\nSelected {len(pairs)} narrative/ticker pairs for validation.\n")

    # Step 2: For each pair, check price movement
    old_hits = 0   # old: ns_score > 0.3 AND >2% move in either direction
    new_hits = 0   # new: impact_score > 0.3 AND >2% move in predicted direction
    convergence_hits = 0
    convergence_total = 0
    total_with_data = 0
    pair_results = []

    for pair in pairs:
        narrative_id = pair["narrative_id"]
        ticker = pair["ticker"]
        direction = pair["direction"]
        impact_score = pair["impact_score"]
        ns_score = pair["ns_score"]
        name = pair["name"]

        print(f"  {name[:50]} / {ticker} (dir={direction}, impact={impact_score:.3f}, ns={ns_score:.3f})")

        # Find peak velocity date
        peak_date = _get_peak_velocity_date(conn, narrative_id)
        if not peak_date:
            print(f"    [SKIP] No snapshots")
            continue

        # Single price fetch — checks both directional and either-direction movement
        pm = _check_price_movement(ticker, peak_date, direction)
        if not pm["data_available"]:
            print(f"    [SKIP] No price data for {ticker}")
            continue

        moved_dir = pm["moved_directional"]
        moved_either = pm["moved_either"]
        max_move = pm["max_either"]  # show actual max move regardless of direction

        total_with_data += 1

        # Old signal: ns_score > 0.3 AND moved >2% either direction
        old_hit = ns_score > 0.3 and moved_either
        if old_hit:
            old_hits += 1

        # New signal: impact_score > 0.3 AND moved >2% in predicted direction
        new_hit = impact_score > 0.3 and moved_dir
        if new_hit:
            new_hits += 1

        # Check convergence (pressure_score > 2.0)
        try:
            conv_row = conn.execute(
                "SELECT pressure_score FROM ticker_convergence WHERE ticker = ?",
                (ticker.upper(),),
            ).fetchone()
            if conv_row and (conv_row[0] or 0.0) > 2.0:
                convergence_total += 1
                if moved_dir:
                    convergence_hits += 1
        except Exception:
            pass

        # Correlation at multiple lags
        correlations = _run_correlation_at_lags(conn, narrative_id, ticker)

        best_lag = None
        if correlations:
            sig_corrs = [c for c in correlations if c["significant"]]
            if sig_corrs:
                best_lag = max(sig_corrs, key=lambda c: abs(c["r"]))
            else:
                best_lag = max(correlations, key=lambda c: abs(c["r"]))

        pair_results.append({
            "narrative_id": narrative_id,
            "name": name,
            "ticker": ticker,
            "direction": direction,
            "impact_score": impact_score,
            "ns_score": ns_score,
            "peak_date": peak_date,
            "moved_directional": moved_dir,
            "moved_either": moved_either,
            "max_move_pct": max_move,
            "old_hit": old_hit,
            "new_hit": new_hit,
            "correlations": correlations,
            "best_lag": best_lag,
        })

        status = "HIT" if new_hit else ("MISS" if impact_score > 0.3 else "LOW-SCORE")
        print(f"    peak={peak_date}, moved={max_move:.1f}%, dir_hit={moved_dir}, status={status}")

    conn.close()

    # Step 3: Compute hit rates
    if total_with_data == 0:
        msg = "No pairs had available price data. Cannot compute hit rates."
        print(f"\n{msg}")
        return {"error": msg, "pairs": len(pairs), "pairs_with_data": 0}

    old_rate = old_hits / total_with_data * 100
    new_rate = new_hits / total_with_data * 100
    conv_rate = (convergence_hits / convergence_total * 100) if convergence_total > 0 else 0.0

    # Find best lead time across all pairs
    all_significant = []
    for pr in pair_results:
        for c in pr.get("correlations", []):
            if c.get("significant"):
                all_significant.append(c)

    best_lead_summary = "N/A"
    if all_significant:
        best_lead_summary = f"{len(all_significant)} significant correlations found"

    # Decision gate
    if new_rate > 60:
        recommendation = "PROCEED — Signal is real. Proceed with Block 3 (feature expansion)."
    elif new_rate >= 40:
        recommendation = "TUNE — Signal is noisy. Adjust feature weights, review asset mapping quality, add more training cycles."
    else:
        recommendation = "RETHINK — Fundamental issues remain. Consider embedding model swap, review convergence threshold, examine narrative-to-ticker mapping."

    # Step 4: Print summary
    print("\n" + "=" * 100)
    print("MID-POINT VALIDATION RESULTS")
    print("=" * 100)
    print(f"\nSample size: {total_with_data} narrative/ticker pairs with price data")
    print(f"  (from {len(pairs)} selected, {len(pairs) - total_with_data} skipped due to missing data)\n")
    print(f"  Old signal hit rate (ns_score > 0.3, >2% either dir):        {old_hits}/{total_with_data} = {old_rate:.1f}%")
    print(f"  New signal hit rate (impact_score > 0.3, >2% predicted dir): {new_hits}/{total_with_data} = {new_rate:.1f}%")
    if convergence_total > 0:
        print(f"  Convergence bonus (pressure > 2.0):                          {convergence_hits}/{convergence_total} = {conv_rate:.1f}%")
    else:
        print(f"  Convergence bonus: No tickers with pressure_score > 2.0")
    print(f"\n  Significant correlations (p<0.05): {best_lead_summary}")
    print(f"\n  RECOMMENDATION: {recommendation}")

    # Detail table
    print(f"\n{'Narrative':<40} {'Ticker':<7} {'Dir':<8} {'Impact':>7} {'Ns':>5} {'Move%':>6} {'Old':>4} {'New':>4}")
    print("-" * 100)
    for pr in pair_results:
        n = pr["name"][:39]
        print(f"{n:<40} {pr['ticker']:<7} {pr['direction']:<8} {pr['impact_score']:>7.3f} {pr['ns_score']:>5.2f} {pr['max_move_pct']:>+6.1f} {'Y' if pr['old_hit'] else 'N':>4} {'Y' if pr['new_hit'] else 'N':>4}")
    print("-" * 100)

    # Step 5: Generate PNG chart for top 5
    _plot_validation_top5(pair_results)

    # Step 6: Append to BUILD_LOG.md
    _append_to_build_log(
        total_with_data, old_hits, old_rate, new_hits, new_rate,
        convergence_hits, convergence_total, conv_rate,
        best_lead_summary, recommendation, pair_results,
    )

    return {
        "pairs": len(pairs),
        "pairs_with_data": total_with_data,
        "old_hit_rate": old_rate,
        "new_hit_rate": new_rate,
        "convergence_hit_rate": conv_rate,
        "recommendation": recommendation,
    }


def _plot_validation_top5(pair_results):
    """Generate velocity vs price overlay PNG for up to 5 best-performing pairs."""
    # Sort by impact_score descending, take top 5 with data
    ranked = [p for p in pair_results if p.get("moved_directional") or p.get("impact_score", 0) > 0.3]
    if not ranked:
        ranked = pair_results
    ranked.sort(key=lambda x: x.get("impact_score", 0), reverse=True)
    top5 = ranked[:5]

    if not top5:
        print("\n[SKIP] No pairs available for chart generation.")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    results_for_plot = []
    for pr in top5:
        vel_data = get_velocity_series(conn, pr["narrative_id"])
        if not vel_data:
            continue
        date_range = sorted(vel_data.keys())
        price_data = get_price_series(pr["ticker"], date_range[0], date_range[-1])
        if not price_data:
            continue

        dates, vels, prices, r_same, n, r_lead = compute_correlation(vel_data, price_data)
        results_for_plot.append({
            "narrative_id": pr["narrative_id"],
            "name": pr["name"],
            "ticker": pr["ticker"],
            "dates": dates,
            "velocities": vels,
            "prices": prices,
            "r_same": r_same,
            "r_lead": r_lead,
            "n": n,
        })

    conn.close()

    if results_for_plot:
        midpoint_path = Path(__file__).parent / "signal_validation_midpoint.png"
        plot_all(results_for_plot, output_path=midpoint_path)


def _append_to_build_log(
    total, old_hits, old_rate, new_hits, new_rate,
    conv_hits, conv_total, conv_rate,
    best_lead_summary, recommendation, pair_results,
):
    """Append validation results to BUILD_LOG.md."""
    build_log = Path(__file__).parent / "BUILD_LOG.md"
    today = datetime.now().strftime("%Y-%m-%d")

    lines = [
        f"\n\n## Mid-Point Validation — Signal Pipeline Phase 0 Rerun — {today}\n",
        f"\n### Hit Rates\n",
        f"| Metric | Hits | Total | Rate |",
        f"|--------|------|-------|------|",
        f"| Old signal (ns_score > 0.3, >2% either dir) | {old_hits} | {total} | {old_rate:.1f}% |",
        f"| New signal (impact_score > 0.3, >2% predicted dir) | {new_hits} | {total} | {new_rate:.1f}% |",
    ]
    if conv_total > 0:
        lines.append(f"| Convergence bonus (pressure > 2.0) | {conv_hits} | {conv_total} | {conv_rate:.1f}% |")
    else:
        lines.append(f"| Convergence bonus | — | 0 | N/A |")

    lines.extend([
        f"\n### Correlation Analysis\n",
        f"- Significant correlations (p<0.05): {best_lead_summary}",
        f"\n### Pair Detail\n",
        f"| Narrative | Ticker | Direction | Impact | Ns | Move% | Old | New |",
        f"|-----------|--------|-----------|--------|-----|-------|-----|-----|",
    ])
    for pr in pair_results:
        n = pr["name"][:35]
        lines.append(
            f"| {n} | {pr['ticker']} | {pr['direction']} | {pr['impact_score']:.3f} "
            f"| {pr['ns_score']:.2f} | {pr['max_move_pct']:+.1f}% "
            f"| {'Y' if pr['old_hit'] else 'N'} | {'Y' if pr['new_hit'] else 'N'} |"
        )

    lines.extend([
        f"\n### Decision Gate\n",
        f"**{recommendation}**\n",
        f"\n### Notes\n",
        f"- Validation run date: {today}",
        f"- Sample: {total} pairs with price data",
        f"- Old signal = hand-tuned ns_score threshold; New signal = directional impact_score from Phases 1-6",
        f"- Chart: `signal_validation_midpoint.png`",
    ])

    try:
        with open(build_log, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        print(f"\nResults appended to {build_log}")
    except Exception as exc:
        print(f"\n[WARN] Failed to append to BUILD_LOG.md: {exc}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--validate":
        run_validation()
    else:
        main()
        print("\n--- Running Mid-Point Validation ---\n")
        run_validation()
