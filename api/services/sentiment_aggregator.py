"""
Composite sentiment aggregator.

Sources:
  - News sentiment:      narrative_signals.direction x confidence (Phase 1)
  - Social sentiment:    StockTwitsAdapter
  - Narrative momentum:  narrative velocity (normalised)

Composite formula:
    0.4 x news_sentiment + 0.3 x social_sentiment + 0.3 x narrative_momentum

Stores hourly aggregations in sentiment_timeseries via repository.
"""

import datetime
import json
import logging
import statistics

logger = logging.getLogger(__name__)


class SentimentAggregator:

    def __init__(self, repository, stocktwits_adapter, finnhub_service=None):
        self._repo = repository
        self._stocktwits = stocktwits_adapter
        self._finnhub = finnhub_service  # reserved for future use

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _news_sentiment(self, ticker: str) -> float:
        """Return [-1, 1] news-derived sentiment for ticker from narrative_signals."""
        try:
            signals = self._repo.get_all_narrative_signals()
        except Exception:
            return 0.0

        scores: list[float] = []
        for sig in signals:
            try:
                nar = self._repo.get_narrative(sig["narrative_id"])
                if not nar or nar.get("suppressed"):
                    continue
                linked_raw = nar.get("linked_assets", "[]")
                linked = json.loads(linked_raw) if isinstance(linked_raw, str) else (linked_raw or [])
                tickers = [a.get("ticker", "") for a in linked if isinstance(a, dict)]
                if ticker not in tickers:
                    continue
                direction = sig.get("direction", "neutral")
                confidence = float(sig.get("confidence", 0) or 0)
                if direction == "bullish":
                    scores.append(confidence)
                elif direction == "bearish":
                    scores.append(-confidence)
                else:
                    scores.append(0.0)
            except Exception:
                continue

        return round(sum(scores) / len(scores), 4) if scores else 0.0

    def _narrative_momentum(self, ticker: str) -> float:
        """Return [-1, 1] momentum from linked narrative velocities."""
        try:
            narratives = self._repo.get_all_active_narratives()
        except Exception:
            return 0.0

        scores: list[float] = []
        for nar in narratives:
            try:
                linked_raw = nar.get("linked_assets", "[]")
                linked = json.loads(linked_raw) if isinstance(linked_raw, str) else (linked_raw or [])
                tickers = [a.get("ticker", "") for a in linked if isinstance(a, dict)]
                if ticker not in tickers:
                    continue
                velocity = float(nar.get("velocity", 0) or 0)
                # Clamp normalised velocity to [-1, 1] (baseline: 10 docs/cycle = 1.0)
                scores.append(max(-1.0, min(1.0, velocity / 10.0)))
            except Exception:
                continue

        return round(sum(scores) / len(scores), 4) if scores else 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_ticker_sentiment(self, ticker: str) -> dict:
        """
        Aggregate sentiment for a single ticker across all sources.

        Returns:
        {
            "ticker": str,
            "composite_score": float,    # -1 to 1
            "news_component": float,
            "social_component": float,
            "momentum_component": float,
            "message_volume_24h": int,
            "sources": {"stocktwits": {...}, "narrative_signals": {...}},
            "spike_detected": bool,
            "computed_at": str,
        }
        """
        news_component = self._news_sentiment(ticker)
        stocktwits_data = self._stocktwits.get_sentiment(ticker)
        social_component = float(stocktwits_data["sentiment_score"]) if stocktwits_data else 0.0
        momentum_component = self._narrative_momentum(ticker)

        composite_score = round(
            0.4 * news_component + 0.3 * social_component + 0.3 * momentum_component,
            4,
        )
        composite_score = max(-1.0, min(1.0, composite_score))

        # Spike detection: > 2 std dev from 7-day rolling mean
        spike_detected = False
        try:
            history = self._repo.get_sentiment_timeseries(ticker, hours=168)
            if len(history) >= 7:
                past = [float(h.get("composite_score") or 0) for h in history]
                mean = statistics.mean(past)
                stdev = statistics.stdev(past) if len(past) > 1 else 0.0
                if stdev > 0 and abs(composite_score - mean) > 2 * stdev:
                    spike_detected = True
        except Exception:
            pass

        return {
            "ticker": ticker,
            "composite_score": composite_score,
            "news_component": round(news_component, 4),
            "social_component": round(social_component, 4),
            "momentum_component": round(momentum_component, 4),
            "message_volume_24h": int(stocktwits_data.get("volume_24h", 0)) if stocktwits_data else 0,
            "sources": {
                "stocktwits": stocktwits_data,
                "narrative_signals": {"news_component": round(news_component, 4)},
            },
            "spike_detected": spike_detected,
            "computed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

    def compute_market_sentiment(self, tickers: list[str]) -> dict:
        """
        Aggregate across a list of tickers for a market-wide gauge.

        Returns:
        {
            "market_score": float,
            "bullish_pct": float,
            "bearish_pct": float,
            "neutral_pct": float,
            "top_bullish": [{"ticker", "score"}],
            "top_bearish": [{"ticker", "score"}],
            "spikes": [{"ticker", "score", "direction"}],
        }
        """
        if not tickers:
            return {
                "market_score": 0.0,
                "bullish_pct": 0.0,
                "bearish_pct": 0.0,
                "neutral_pct": 0.0,
                "top_bullish": [],
                "top_bearish": [],
                "spikes": [],
            }

        results: list[dict] = []
        for ticker in tickers:
            try:
                r = self.compute_ticker_sentiment(ticker)
                results.append(r)
            except Exception:
                results.append({
                    "ticker": ticker, "composite_score": 0.0, "spike_detected": False,
                })

        scores = [float(r.get("composite_score") or 0) for r in results]
        market_score = round(sum(scores) / len(scores), 4) if scores else 0.0

        total = len(scores) or 1
        bullish = sum(1 for s in scores if s > 0.1)
        bearish = sum(1 for s in scores if s < -0.1)
        neutral = total - bullish - bearish

        sorted_asc = sorted(results, key=lambda r: float(r.get("composite_score") or 0))
        top_bullish = [
            {"ticker": r["ticker"], "score": float(r.get("composite_score") or 0)}
            for r in reversed(sorted_asc[-5:])
            if float(r.get("composite_score") or 0) > 0
        ][:3]
        top_bearish = [
            {"ticker": r["ticker"], "score": float(r.get("composite_score") or 0)}
            for r in sorted_asc[:5]
            if float(r.get("composite_score") or 0) < 0
        ][:3]
        spikes = [
            {
                "ticker": r["ticker"],
                "score": float(r.get("composite_score") or 0),
                "direction": "bullish" if float(r.get("composite_score") or 0) > 0 else "bearish",
            }
            for r in results
            if r.get("spike_detected")
        ]

        return {
            "market_score": market_score,
            "bullish_pct": round(bullish / total * 100, 1),
            "bearish_pct": round(bearish / total * 100, 1),
            "neutral_pct": round(neutral / total * 100, 1),
            "top_bullish": top_bullish,
            "top_bearish": top_bearish,
            "spikes": spikes,
        }
