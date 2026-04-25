"""
Retired outbound posting module.

Phase 3 removed pipeline-triggered posting. This module is kept only for
legacy test coverage and local analysis helpers; dispatch_tweets is now a no-op.
"""

import json
import logging
import random
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Voice: Canary Intel
#
# - No emojis. No em dashes. No hashtags.
# - Cashtags for tickers ($NVDA not #NVDA).
# - Tone: sharp analyst who sees things early. Between interpretation
#   and opinionated. Never sounds like a bot or a press release.
# - Soften predictions: "tend to", "worth watching", "historically".
# - Vary sentence length. Favor short.
# - Never say "our system", "we detected", "our analysis".
# - Max 280 characters per tweet. Truncate if needed.
# ---------------------------------------------------------------------------

MAX_TWEET_LENGTH = 280

# Macro patterns (sector convergence, ticker convergence, etc.) change slowly.
# 48h cooldown per unique pattern key prevents the same observation every cycle.
MACRO_COOLDOWN_HOURS = 48


class ChangeDetector:
    """Determines which narratives have tweetable changes."""

    MIN_DOC_COUNT = 8
    VELOCITY_SPIKE_RATIO = 1.5
    BURST_RATIO_THRESHOLD = 2.5
    EXCLUDED_STAGES = {"Declining", "Dormant"}
    NARRATIVE_COOLDOWN_HOURS = 24
    NARRATIVE_ROLLING_WINDOW_DAYS = 7
    NARRATIVE_ROLLING_MAX_POSTS = 3

    def __init__(self, repository, *, require_tickers=True,
                 max_new_per_cycle=3):
        self.repo = repository
        self.require_tickers = require_tickers
        self.max_new_per_cycle = max_new_per_cycle

    MAX_NEW_PER_CYCLE = 3

    def find_tweetable_events(self, active_narratives):
        """
        Compare current narrative state against last tweeted state.
        Returns list of (narrative, trigger_type, priority) sorted
        by priority descending.

        Cold-start protection: new_narrative events are capped at
        MAX_NEW_PER_CYCLE, ranked by ns_score so only the strongest
        narratives get introduced. Real changes (stage transitions,
        velocity spikes) always take priority.
        """
        change_events = []
        new_events = []
        now = datetime.now(timezone.utc)

        for narrative in active_narratives:
            nid = narrative["narrative_id"]

            if narrative.get("stage") in self.EXCLUDED_STAGES:
                continue

            linked = parse_tickers(narrative.get("linked_assets"))
            if self.require_tickers and not linked:
                continue

            if (narrative.get("document_count") or 0) < self.MIN_DOC_COUNT:
                continue

            last_tweet = self.repo.get_last_tweet_for_narrative(nid)

            if last_tweet is None:
                new_events.append((narrative, "new_narrative", 50))
                continue

            # Rolling 7-day cap (hard limit, no exceptions)
            seven_days_ago = (
                now - timedelta(days=self.NARRATIVE_ROLLING_WINDOW_DAYS)
            ).isoformat()
            posts_in_window = self.repo.get_tweet_count_for_narrative_since(
                nid, seven_days_ago)
            if posts_in_window >= self.NARRATIVE_ROLLING_MAX_POSTS:
                continue

            # 24h cooldown flag (stage_change exempt, checked below)
            hours_since_last = _hours_since(last_tweet.get("posted_at", ""))
            _cooldown_active = hours_since_last < self.NARRATIVE_COOLDOWN_HOURS

            last_metrics = json.loads(last_tweet.get("metrics_snapshot") or "{}")
            last_stage = last_metrics.get("stage")
            last_velocity = last_metrics.get("velocity", 0)
            last_doc_count = last_metrics.get("document_count", 0)

            current_stage = narrative.get("stage")
            current_velocity = narrative.get("velocity") or 0
            current_burst = narrative.get("burst_ratio") or 0

            # Stage transition (exempt from 24h cooldown)
            if current_stage != last_stage and last_stage is not None:
                priority = 80 if current_stage == "Mature" else 60
                change_events.append((narrative, "stage_change", priority))
                continue

            # All triggers below are subject to the 24h cooldown
            if _cooldown_active:
                continue

            # Velocity spike
            if (last_velocity > 0
                    and current_velocity / last_velocity > self.VELOCITY_SPIKE_RATIO):
                change_events.append((narrative, "velocity_spike", 70))
                continue

            # Burst detection
            if current_burst > self.BURST_RATIO_THRESHOLD:
                change_events.append((narrative, "velocity_spike", 75))
                continue

            # Coverage doubled since last tweet
            doc_count = narrative.get("document_count") or 0
            if last_doc_count > 0 and doc_count > last_doc_count * 2:
                change_events.append((narrative, "coverage_surge", 40))

        # Cap new_narrative events: keep only the top N by ns_score
        new_events.sort(
            key=lambda x: float(x[0].get("ns_score") or 0), reverse=True,
        )
        cap = self.max_new_per_cycle or self.MAX_NEW_PER_CYCLE
        new_events = new_events[:cap]

        # Real changes always rank above new introductions
        events = change_events + new_events
        events.sort(key=lambda x: x[2], reverse=True)
        return events


class TweetComposer:
    """Translates narrative data into natural language tweets."""

    def compose(self, narrative, trigger_type, last_tweet=None):
        """
        Returns list of tweet strings.
        Single element = simple tweet. Multiple = thread.
        """
        tickers = _format_tickers(narrative)
        name = narrative.get("name", "Unknown")
        doc_count = narrative.get("document_count", 0)
        stage = narrative.get("stage", "")
        short = _short_name(name)

        if trigger_type == "new_narrative":
            return self._new_narrative(name, short, tickers, doc_count, stage)
        elif trigger_type == "velocity_spike":
            burst = narrative.get("burst_ratio") or 0
            return self._velocity_spike(short, tickers, doc_count, burst)
        elif trigger_type == "stage_change":
            old_metrics = json.loads(
                (last_tweet or {}).get("metrics_snapshot") or "{}")
            old_stage = old_metrics.get("stage", "unknown")
            return self._stage_change(short, tickers, doc_count, stage,
                                      old_stage)
        elif trigger_type == "coverage_surge":
            old_metrics = json.loads(
                (last_tweet or {}).get("metrics_snapshot") or "{}")
            old_count = old_metrics.get("document_count", 0)
            return self._coverage_surge(short, tickers, doc_count, old_count)
        return [_truncate(f"{short}. {doc_count} sources tracking. {tickers}")]

    def compose_quote_update(self, narrative, trigger_type):
        """Compose text for a quote-tweet update on an existing narrative."""
        tickers = _format_tickers(narrative)
        doc_count = narrative.get("document_count", 0)
        stage = narrative.get("stage", "")
        short = _short_name(narrative.get("name", ""))

        if trigger_type == "stage_change":
            if stage == "Mature":
                pool = [
                    f"Update: this went mainstream. {doc_count} sources now. {tickers}",
                    f"This matured. {doc_count} outlets covering it. {tickers}",
                ]
            elif stage == "Declining":
                pool = [
                    f"Update: coverage dropping off on this one. {tickers}",
                    f"This story is fading. Sources are moving on. {tickers}",
                ]
            else:
                pool = [
                    f"Update: {short} shifted. Now {_stage_word(stage)}. {tickers}",
                ]
        elif trigger_type == "velocity_spike":
            pool = [
                f"Update: this spiked again. {doc_count} sources now. {tickers}",
                f"Still accelerating. {doc_count} sources and climbing. {tickers}",
            ]
        elif trigger_type == "coverage_surge":
            pool = [
                f"Coverage doubled on this one. {doc_count} sources now. {tickers}",
                f"Update: source count keeps growing. {doc_count} now. {tickers}",
            ]
        else:
            pool = [f"Update: {doc_count} sources now. {tickers}"]

        return _truncate(random.choice(pool))

    def compose_thread(self, narratives_for_sector, sector_name, tickers):
        """Compose a thread for converging narratives on a sector."""
        count = len(narratives_for_sector)
        openers = [
            (f"{count} separate stories pointing at {sector_name} right now. "
             f"None of them reference each other. That kind of independent "
             f"convergence tends to matter. {tickers} thread-"),
            (f"Something building in {sector_name}. {count} unrelated "
             f"narratives all circling the same names. {tickers} thread-"),
        ]
        tweets = [_truncate(random.choice(openers))]

        for i, n in enumerate(narratives_for_sector[:4]):
            name = _short_name(n.get("name", ""))
            dc = n.get("document_count", 0)
            t = _format_tickers(n)
            tweets.append(_truncate(f"{i + 1}. {name}. {dc} sources. {t}"))

        return tweets

    # -- trigger-specific composers --

    def _new_narrative(self, name, short, tickers, doc_count, stage):
        use_thread = doc_count > 50 and len(parse_tickers_raw(tickers)) >= 3
        sw = _stage_word(stage)

        single_pool = [
            f"New one forming. {name}. {doc_count} sources picked this up "
            f"and it's still {sw}. {tickers}",
            f"Something worth watching. {name} just crossed {doc_count} "
            f"sources. {tickers}",
            f"Worth tracking: {name}. {doc_count} independent sources "
            f"and counting. {tickers}",
            f"{name}. Started showing up across {doc_count} outlets "
            f"recently. Still {sw}. {tickers}",
            f"Picking up a new thread. {name} with {doc_count} sources "
            f"already on it. {tickers}",
        ]

        if use_thread:
            head = _truncate(
                f"New narrative forming with real traction. {name}. "
                f"{doc_count} sources already on this and it's still "
                f"{sw}. {tickers} thread-"
            )
            body = _truncate(
                f"Coverage is broad. {doc_count} outlets across "
                f"multiple tiers. Linked names: {tickers}. "
                f"This is still {sw} so it could go either way "
                f"but the source diversity stands out."
            )
            return [head, body]

        return [_truncate(random.choice(single_pool))]

    def _velocity_spike(self, short, tickers, doc_count, burst_ratio):
        if burst_ratio > 3.0:
            pool = [
                f"That {short} story just accelerated hard. Coverage "
                f"spiked across {doc_count} sources. {tickers}",
                f"{short} picked up real momentum in the last few hours. "
                f"{doc_count} sources tracking now. {tickers}",
                f"Sudden jump in coverage on {short}. This went from "
                f"background noise to {doc_count} sources fast. {tickers}",
            ]
        else:
            pool = [
                f"{short} is gaining traction. Source count hit "
                f"{doc_count} and still climbing. {tickers}",
                f"More outlets picking up {short}. Now at "
                f"{doc_count} sources. {tickers}",
                f"{short} keeps spreading. {doc_count} sources "
                f"on it now. {tickers}",
            ]
        return [_truncate(random.choice(pool))]

    def _stage_change(self, short, tickers, doc_count, new_stage, old_stage):
        if new_stage == "Mature":
            pool = [
                f"{short} isn't early anymore. {doc_count} sources deep "
                f"and this has gone mainstream. {tickers}",
                f"This hit critical mass. {short} across {doc_count} "
                f"sources now. {tickers}",
                f"If you haven't been following {short} you're behind. "
                f"{doc_count} outlets on this. {tickers}",
            ]
        elif new_stage == "Declining":
            pool = [
                f"Coverage on {short} is thinning out. The narrative "
                f"ran its course. {tickers}",
                f"{short} peaked. Sources are dropping this story. "
                f"{tickers}",
            ]
        elif new_stage == "Growing":
            pool = [
                f"{short} is picking up. Moved past niche coverage to "
                f"{doc_count} sources. {tickers}",
                f"Early signal on {short} is getting confirmed by wider "
                f"coverage. {doc_count} outlets now. {tickers}",
            ]
        else:
            pool = [
                f"Shift in {short}. Coverage pattern changed with "
                f"{doc_count} sources tracking. {tickers}",
            ]
        return [_truncate(random.choice(pool))]

    def _coverage_surge(self, short, tickers, doc_count, old_count):
        pool = [
            f"{short} coverage doubled. Was {old_count} sources, "
            f"now {doc_count}. {tickers}",
            f"Media pile-on: {short} went from {old_count} to "
            f"{doc_count} sources. {tickers}",
            f"{short} is snowballing. {old_count} to {doc_count} "
            f"sources. {tickers}",
        ]
        return [_truncate(random.choice(pool))]


class MacroPatternDetector:
    """Scans active narratives for cross-narrative macro patterns."""

    def __init__(self, repository):
        self.repo = repository

    def detect(self, active_narratives):
        """
        Returns list of (pattern_type, observation_data) tuples,
        sorted by interestingness. Never raises.
        """
        patterns = []
        try:
            patterns.extend(self._sector_convergence(active_narratives))
        except Exception as exc:
            logger.debug("Macro: sector_convergence failed: %s", exc)
        try:
            patterns.extend(self._stage_clustering(active_narratives))
        except Exception as exc:
            logger.debug("Macro: stage_clustering failed: %s", exc)
        try:
            patterns.extend(self._sector_direction(active_narratives))
        except Exception as exc:
            logger.debug("Macro: sector_direction failed: %s", exc)
        try:
            patterns.extend(self._volume_shift(active_narratives))
        except Exception as exc:
            logger.debug("Macro: volume_shift failed: %s", exc)
        try:
            patterns.extend(self._ticker_convergence())
        except Exception as exc:
            logger.debug("Macro: ticker_convergence failed: %s", exc)

        # Sort by priority descending, deduplicate by type
        patterns.sort(key=lambda x: x[1].get("priority", 0), reverse=True)
        return patterns

    def _sector_convergence(self, active_narratives):
        """Multiple narratives hitting the same sector independently."""
        try:
            from api.sector_map import SECTOR_MAP
        except ImportError:
            return []

        sector_narratives = {}
        for n in active_narratives:
            if n.get("stage") in ("Declining", "Dormant"):
                continue
            tickers = parse_tickers(n.get("linked_assets"))
            sectors_seen = set()
            for t in tickers:
                sector = SECTOR_MAP.get(t)
                if sector and sector not in sectors_seen:
                    sectors_seen.add(sector)
                    sector_narratives.setdefault(sector, []).append(n)

        results = []
        for sector, narratives in sector_narratives.items():
            if len(narratives) >= 3:
                names = [_safe_text(n.get("name", ""))[:50] for n in narratives[:3]]
                tickers = set()
                ticker_map = {}
                ticker_descriptions = {}
                for n in narratives:
                    short = _safe_text(n.get("name", ""))[:40]
                    assets = _parse_linked_assets_full(n.get("linked_assets"))
                    for a in assets:
                        t = a.get("ticker", "")
                        if SECTOR_MAP.get(t) == sector:
                            tickers.add(t)
                            ticker_map.setdefault(t, []).append(short)
                            if t not in ticker_descriptions and a.get("asset_name"):
                                ticker_descriptions[t] = _safe_text(
                                    a["asset_name"])
                map_str = "; ".join(
                    f"${t} ({ticker_descriptions.get(t, t)}) -> "
                    f"{', '.join(ns)}"
                    for t, ns in ticker_map.items()
                )
                results.append(("sector_convergence", {
                    "sector": sector,
                    "narrative_count": len(narratives),
                    "narrative_names": names,
                    "tickers": sorted(tickers)[:4],
                    "ticker_descriptions": ticker_descriptions,
                    "ticker_narrative_map": map_str,
                    "priority": len(narratives) * 20,
                }))
        return results

    def _stage_clustering(self, active_narratives):
        """Unusual concentration of stage changes or new narratives."""
        from collections import Counter
        try:
            from api.sector_map import SECTOR_MAP
        except ImportError:
            return []

        # Count emerging/growing by sector
        emerging = [n for n in active_narratives
                    if n.get("stage") in ("Emerging", "Growing")]
        if len(emerging) < 3:
            return []

        sector_new = Counter()
        for n in emerging:
            tickers = parse_tickers(n.get("linked_assets"))
            for t in tickers:
                sector = SECTOR_MAP.get(t)
                if sector:
                    sector_new[sector] += 1
                    break  # one sector per narrative

        results = []
        for sector, count in sector_new.most_common(2):
            if count >= 2:
                results.append(("stage_clustering", {
                    "sector": sector,
                    "new_count": count,
                    "total_emerging": len(emerging),
                    "priority": count * 15,
                }))
        return results

    def _sector_direction(self, active_narratives):
        """All narratives in a sector accelerating or decelerating together."""
        try:
            from api.sector_map import SECTOR_MAP
        except ImportError:
            return []

        sector_velocities = {}
        for n in active_narratives:
            if n.get("stage") in ("Declining", "Dormant"):
                continue
            velocity = n.get("velocity") or 0
            tickers = parse_tickers(n.get("linked_assets"))
            for t in tickers:
                sector = SECTOR_MAP.get(t)
                if sector:
                    sector_velocities.setdefault(sector, []).append(velocity)
                    break

        results = []
        for sector, velocities in sector_velocities.items():
            if len(velocities) < 3:
                continue
            avg_vel = sum(velocities) / len(velocities)
            # All accelerating or all decelerating?
            if avg_vel > 0.15:
                results.append(("sector_accelerating", {
                    "sector": sector,
                    "avg_velocity": avg_vel,
                    "narrative_count": len(velocities),
                    "priority": int(avg_vel * 100),
                }))
            elif avg_vel < 0.05 and len(velocities) >= 3:
                results.append(("sector_decelerating", {
                    "sector": sector,
                    "avg_velocity": avg_vel,
                    "narrative_count": len(velocities),
                    "priority": 15,
                }))
        return results

    def _volume_shift(self, active_narratives):
        """Overall narrative pipeline activity level."""
        total = len(active_narratives)
        emerging = sum(1 for n in active_narratives
                       if n.get("stage") == "Emerging")
        mature = sum(1 for n in active_narratives
                     if n.get("stage") == "Mature")

        results = []
        if emerging >= 5:
            results.append(("high_volume_new", {
                "emerging_count": emerging,
                "total": total,
                "priority": emerging * 10,
            }))
        if total > 0 and mature / total > 0.7:
            results.append(("mature_dominated", {
                "mature_count": mature,
                "total": total,
                "priority": 20,
            }))
        return results

    def _ticker_convergence(self):
        """Multiple independent narratives converging on same ticker."""
        try:
            convs = self.repo.get_top_convergences(limit=5)
        except Exception:
            return []

        results = []
        for c in convs:
            d = dict(c) if hasattr(c, "keys") else c
            ticker = d.get("ticker", "")
            if ticker.startswith("TOPIC:"):
                continue
            nids = []
            try:
                nids = json.loads(d.get("contributing_narrative_ids") or "[]")
            except (json.JSONDecodeError, TypeError):
                pass
            pressure = d.get("pressure_score") or 0
            if len(nids) >= 2 and pressure >= 0.5:
                # Look up narrative names and company name
                narrative_names = []
                company_name = ticker
                for nid in nids[:3]:
                    try:
                        n = self.repo.get_narrative(nid)
                        if n:
                            narrative_names.append(
                                _safe_text(n.get("name", ""))[:40])
                            if company_name == ticker:
                                for a in _parse_linked_assets_full(
                                        n.get("linked_assets")):
                                    if a.get("ticker") == ticker:
                                        company_name = _safe_text(
                                            a.get("asset_name", ticker))
                                        break
                    except Exception:
                        continue
                results.append(("ticker_convergence", {
                    "ticker": ticker,
                    "company_name": company_name,
                    "narrative_count": len(nids),
                    "narrative_names": narrative_names,
                    "pressure_score": d.get("pressure_score", 0),
                    "direction": "bearish" if d.get("direction_agreement", 0) < 0 else "bullish",
                    "priority": int((d.get("pressure_score") or 0) * 40),
                }))
        return results


class MacroTweetComposer:
    """Uses Haiku to compose 280-char macro tweets from pattern observations."""

    _SYSTEM_PROMPT = (
        "You are Canary Intel. Write ONE tweet, max 280 characters.\n\n"
        "RULES:\n"
        "- No emojis. No em dashes. No hashtags. Cashtags for tickers.\n"
        "- Sharp, confident, slightly provocative.\n"
        "- Simple language a smart 20-year-old understands. Short sentences.\n"
        "- When you mention a ticker, ALWAYS say what the company does "
        "in the same sentence. 'Equity Residential ($EQR), one of the "
        "biggest apartment landlords' — not just '$EQR'. Assume the "
        "reader has never heard of any of these companies.\n"
        "- State the 'so what'. Why should the reader care? What does "
        "this pattern mean for their money, their portfolio, their "
        "understanding of the market? One sentence is enough.\n"
        "- ONE idea per tweet. Pick the single sharpest observation "
        "and commit to it. Do not try to cover the whole pattern.\n"
        "- Use plain sector names. Say 'apartment landlords' not "
        "'Real Estate'. Say 'banks' not 'Financials'. Say 'drug makers' "
        "not 'Health Care'. Write like a human, not a terminal.\n"
        "- Never say 'our system', 'we detected', 'our analysis'.\n"
        "- Never use the word 'narrative'. Say 'story', 'thread', 'theme'.\n"
        "- Never mention methodology, signal strength, spread rate, "
        "cohesion, velocity, or any internal metric.\n"
        "- Round percentages. Use 'roughly', 'around', 'nearly'.\n"
        "- NEVER invent market claims not in the data. No 'repositioning', "
        "'pricing in', 'accumulating', institutional activity, insider "
        "behavior, or whale movements.\n"
        "- ONLY use tickers provided in the data. Never invent tickers.\n"
        "- Only attribute a ticker to the story it belongs to per the "
        "mapping. Do not imply a ticker is part of a story it isn't.\n"
        "- VARY your structure. Sometimes lead with a question. Sometimes "
        "a bold claim. Sometimes a contrast. Never the same pattern "
        "twice.\n\n"
        "RESPOND WITH ONLY THE TWEET TEXT. Nothing else. No quotes. "
        "No labels. No preamble. Max 280 characters."
    )

    def __init__(self, llm_client):
        self.llm = llm_client

    def compose(self, pattern_type, data):
        """Generate a 280-char macro tweet. Falls back to template."""
        prompt = self._build_prompt(pattern_type, data)
        raw = self.llm.call_haiku("compose_draft", "macro", prompt,
                                  max_tokens=128)
        raw = (raw or "").strip().strip('"').strip("'")

        if raw and len(raw) <= 280 and len(raw) >= 30:
            editorial_ok, flagged = _check_editorial_claims(raw)
            if not editorial_ok:
                logger.warning(
                    "Macro tweet has unsupported editorial claims: %s "
                    "-- using template", ", ".join(flagged))
                return self._template_fallback(pattern_type, data)
            return raw

        if raw and len(raw) > 280:
            cut = raw[:277]
            last_period = cut.rfind(".")
            if last_period > 100:
                truncated = cut[:last_period + 1]
                editorial_ok, flagged = _check_editorial_claims(truncated)
                if not editorial_ok:
                    logger.warning(
                        "Macro tweet has unsupported editorial claims: %s "
                        "-- using template", ", ".join(flagged))
                    return self._template_fallback(pattern_type, data)
                return truncated

        logger.warning("Macro tweet compose failed, using template")
        return self._template_fallback(pattern_type, data)

    def _build_prompt(self, pattern_type, data):
        desc = {
            "sector_convergence": self._desc_sector_convergence(data),
            "stage_clustering": self._desc_stage_clustering(data),
            "sector_accelerating": self._desc_sector_direction(data, True),
            "sector_decelerating": self._desc_sector_direction(data, False),
            "high_volume_new": self._desc_volume(data, True),
            "mature_dominated": self._desc_volume(data, False),
            "ticker_convergence": self._desc_ticker_convergence(data),
        }.get(pattern_type, f"Interesting macro pattern: {pattern_type}")

        return f"{self._SYSTEM_PROMPT}\n\nPATTERN DATA:\n{desc}"

    def _desc_sector_convergence(self, data):
        allowed = ", ".join(f"${t}" for t in data.get("tickers", []))
        return (
            f"{data.get('narrative_count', 0)} completely unrelated stories "
            f"are hitting {data.get('sector', 'a sector')} stocks "
            f"at the same time.\n"
            f"Stories: {'; '.join(data.get('narrative_names', []))}.\n"
            f"Ticker-story mapping (use this for accuracy):\n"
            f"{data.get('ticker_narrative_map', 'unavailable')}\n"
            f"ALLOWED TICKERS (use ONLY these): {allowed}\n"
            f"These stories emerged independently. The interesting "
            f"part is that the SECTOR is getting hit from multiple "
            f"angles at once. Focus on what that means for the "
            f"sector, not individual tickers."
        )

    def _desc_stage_clustering(self, data):
        return (
            f"{data.get('new_count', 0)} new stories just emerged in "
            f"{data.get('sector', 'one sector')} out of "
            f"{data.get('total_emerging', 0)} total new stories. "
            f"Unusual concentration in one area."
        )

    def _desc_sector_direction(self, data, accelerating):
        if accelerating:
            return (
                f"All {data.get('narrative_count', 0)} stories touching "
                f"{data.get('sector', 'a sector')} stocks are picking up "
                f"speed at the same time. When an entire sector's coverage "
                f"accelerates together, it usually means something "
                f"structural is shifting."
            )
        return (
            f"Every story touching {data.get('sector', 'a sector')} "
            f"stocks is losing steam simultaneously. "
            f"{data.get('narrative_count', 0)} stories fading together."
        )

    def _desc_volume(self, data, is_high):
        if is_high:
            return (
                f"{data.get('emerging_count', 0)} brand new stories "
                f"appeared this cycle out of {data.get('total', 0)} total. "
                f"That's an unusual burst of fresh themes forming."
            )
        return (
            f"{data.get('mature_count', 0)} out of {data.get('total', 0)} "
            f"active stories are mature. Very few new ones forming. "
            f"Markets tend to get complacent in these stretches."
        )

    def _desc_ticker_convergence(self, data):
        company = data.get("company_name", data.get("ticker", "???"))
        ticker = data.get("ticker", "???")
        names = data.get("narrative_names", [])
        return (
            f"{company} (${ticker}) has "
            f"{data.get('narrative_count', 0)} completely independent "
            f"stories converging on it. Direction leans "
            f"{data.get('direction', 'mixed')}.\n"
            f"Stories: {'; '.join(names)}.\n"
            f"These stories emerged separately. The interesting "
            f"part is that one company is at the center of multiple "
            f"unrelated themes. Focus on why that matters and "
            f"what it could mean next."
        )

    def _template_fallback(self, pattern_type, data):
        """Deterministic fallback tweets — Canary Intel voice."""
        sector_raw = data.get("sector", "one sector")
        sector = sector_raw.lower() if sector_raw[0:1].isupper() else sector_raw

        cashtags = " ".join(f"${t}" for t in data.get("tickers", [])[:3])
        company = data.get("company_name", data.get("ticker", "???"))
        ticker = data.get("ticker", "???")
        n_count = data.get("narrative_count", 0)

        templates = {
            "sector_convergence": (
                f"{n_count} unrelated stories building around "
                f"{sector} stocks at the same time. "
                f"When a sector draws independent attention from "
                f"this many angles, it tends to move. {cashtags}"
            ),
            "stage_clustering": (
                f"{data.get('new_count', 0)} new stories just surfaced "
                f"in {sector}. That is unusual concentration in one "
                f"corner of the market."
            ),
            "sector_accelerating": (
                f"Every story touching {sector} stocks is picking up "
                f"speed right now. Whole sector heating up at once."
            ),
            "sector_decelerating": (
                f"Coverage across {sector} stocks is thinning out. "
                f"Every story fading at the same time."
            ),
            "high_volume_new": (
                f"{data.get('emerging_count', 0)} new stories just "
                f"appeared. That is an unusual burst of fresh themes "
                f"forming at once."
            ),
            "mature_dominated": (
                f"Most active stories are mature. Few new ones forming. "
                f"Quiet stretches like this tend to precede sharp moves."
            ),
            "ticker_convergence": (
                f"{company} (${ticker}) is drawing attention from "
                f"{n_count} completely separate stories. "
                f"Direction leans {data.get('direction', 'mixed')}."
            ),
        }
        text = templates.get(pattern_type, "Macro pattern detected.")
        return text[:280]


class HaikuDraftComposer:
    """Uses Haiku to generate Canary Intel analysis drafts for X Articles."""

    # Data is sanitized via _safe_text() before injection into the prompt
    _SYSTEM_PROMPT = (
        "You are Canary Intel, a sharp market narrative analyst writing "
        "posts for X (Twitter). Your audience is broad: retail investors, "
        "curious professionals, anyone scrolling. Your job is to make "
        "them stop scrolling and read.\n\n"
        "VOICE RULES (mandatory):\n"
        "- No emojis. No em dashes. No hashtags.\n"
        "- Use cashtags for tickers ($NVDA not NVDA).\n"
        "- Tone: confident, sharp, slightly provocative. You see things "
        "early and you say it plainly.\n"
        "- Simple language. No jargon. A smart 20-year-old should "
        "understand every sentence.\n"
        "- Short sentences. Punchy. Mix one-liners with slightly longer "
        "observations. Never two long sentences back to back.\n"
        "- Never say 'our system', 'we detected', 'our analysis', "
        "'our data', 'worth watching', 'worth noting'.\n"
        "- Never use the word 'narrative'. Say 'story', 'thread', 'theme'.\n\n"
        "STRUCTURE RULES (mandatory):\n"
        "- FIRST SENTENCE is the hook. Lead with the single most "
        "interesting or surprising observation. Not the setup. Not "
        "background. The thing that makes someone stop scrolling.\n"
        "- ANALYZE each ticker's move. Explain WHY it moved in one "
        "sentence. e.g. '$AIG down roughly 2% as investors quietly "
        "price in AI liability risk'. Not just the number.\n"
        "- For each price move, briefly acknowledge one or two plausible "
        "drivers in the SAME sentence. Never devote a full paragraph to "
        "explaining one stock.\n"
        "- PRICE DATA is sacred. When the PRICE CONTEXT field gives a "
        "specific number (e.g. '$TSLA up 4.2% over 5 days ($385.95)'), "
        "you MUST reproduce the ticker, direction, percentage, and price "
        "EXACTLY as given. Do not round, paraphrase, or restate price "
        "data. Copy it verbatim into your text.\n"
        "- For NON-PRICE numbers (source counts, general observations), "
        "round to the nearest whole number and hedge: 'roughly 120 "
        "sources', 'around 11 outlets'.\n"
        "- NEVER invent market claims not supported by the data provided. "
        "Do not say players are 'repositioning', 'pricing in', "
        "'accumulating', or make any claim about institutional activity, "
        "insider behavior, or whale movements. You only know what the "
        "data block tells you.\n"
        "- LAST SENTENCE creates tension or anticipation. Not a passive "
        "'worth watching'. Something forward-looking that makes the "
        "reader want to check back. e.g. 'If this holds into earnings "
        "season, the repricing has barely started.'\n"
        "- Do NOT explain methodology. No sentences about spread rate, "
        "consistency, signal strength, coverage patterns, source tiers, "
        "or how the analysis works. These are internal. The reader "
        "never sees them.\n"
        "- Never show raw decimal metrics (no '0.686' or '0.1255').\n"
        "- Source count is the only precise number allowed.\n"
        "- ONLY use tickers from the TICKERS field in the data. Never "
        "invent, add, or reference any ticker not provided. If only "
        "one ticker is provided, only discuss that one.\n\n"
        "HEADLINE RULES:\n"
        "- Max 80 chars, no period.\n"
        "- Create curiosity. The reader should NEED to read the body.\n"
        "- Bad: 'AI Risk Story Splits Winners and Losers'\n"
        "- Good: 'Insurance Stocks Are Moving on AI Liability Risk'\n"
        "- Good: 'Nobody Is Talking About the Crypto Lobbying Backlash'\n\n"
        "OUTPUT FORMAT (mandatory — follow exactly):\n"
        "HEADLINE: <headline>\n"
        "---\n"
        "<body text, 400-600 chars. Dense, engaging, no filler. "
        "End with the cashtags on their own line.>\n\n"
        "Do NOT include any preamble, explanation, or text outside "
        "this format."
    )

    def __init__(self, llm_client):
        self.llm = llm_client

    def compose(self, narrative, trigger_type, price_context, last_tweet=None):
        """
        Generate a Haiku-written draft. Returns (headline, body) tuple.
        Falls back to template on LLM failure.
        """
        nid = narrative["narrative_id"]
        prompt = self._build_prompt(narrative, trigger_type, price_context,
                                    last_tweet)
        raw = self.llm.call_haiku("compose_draft", nid, prompt, max_tokens=1024)

        headline, body = self._parse_response(raw)
        if headline and body:
            # Validate factual accuracy before accepting the draft
            full_draft = f"{headline}\n{body}"

            price_ok, price_reasons = _validate_price_accuracy(
                full_draft, price_context)
            if not price_ok:
                logger.warning(
                    "Haiku draft for %s failed price validation: %s "
                    "-- using template", nid, "; ".join(price_reasons))
                return self._template_fallback(
                    narrative, trigger_type, price_context)

            editorial_ok, flagged = _check_editorial_claims(full_draft)
            if not editorial_ok:
                logger.warning(
                    "Haiku draft for %s has unsupported editorial claims: "
                    "%s -- using template", nid, ", ".join(flagged))
                return self._template_fallback(
                    narrative, trigger_type, price_context)

            return headline, body

        # Fallback: template-based
        logger.warning("Haiku draft parse failed for %s, using template", nid)
        return self._template_fallback(narrative, trigger_type, price_context)

    def _build_prompt(self, narrative, trigger_type, price_context, last_tweet):
        name = _safe_text(narrative.get("name", "Unknown"))
        stage = narrative.get("stage", "Unknown")
        doc_count = narrative.get("document_count", 0)
        velocity = narrative.get("velocity") or 0
        burst = narrative.get("burst_ratio") or 0
        ns_score = narrative.get("ns_score") or 0
        cohesion = narrative.get("cohesion") or 0
        tickers = _format_tickers(narrative)

        tags_raw = narrative.get("topic_tags")
        try:
            tags = json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw or [])
        except (json.JSONDecodeError, TypeError):
            tags = []
        topic_str = ", ".join(_safe_text(str(t)) for t in tags[:5]) if tags else "none"

        # Map trigger to plain English
        trigger_desc = {
            "new_narrative": "This is a newly detected story.",
            "stage_change": f"This story just shifted phase to {_stage_word(stage)}.",
            "velocity_spike": "This story just saw a sudden spike in coverage.",
            "coverage_surge": "Source count on this story roughly doubled recently.",
        }.get(trigger_type, "This story had a notable change.")

        # Build last-tweet context for updates
        update_ctx = ""
        if last_tweet:
            old_metrics = {}
            try:
                old_metrics = json.loads(last_tweet.get("metrics_snapshot") or "{}")
            except (json.JSONDecodeError, TypeError):
                pass
            old_count = old_metrics.get("document_count", 0)
            old_stage = old_metrics.get("stage", "")
            if old_count or old_stage:
                update_ctx = (
                    f"\nPrevious state: {old_count} sources, phase was "
                    f"{_stage_word(old_stage)}."
                )

        data_block = (
            f"STORY: {name}\n"
            f"TRIGGER: {trigger_desc}\n"
            f"SOURCES REPORTING: {doc_count}\n"
            f"COVERAGE PHASE: {_stage_word(stage)}\n"
            f"SPREAD RATE: {velocity:.4f} (higher = faster pickup)\n"
            f"SUDDEN ACCELERATION: {burst:.2f}x (above 2.5 = notable spike)\n"
            f"SIGNAL STRENGTH: {ns_score:.3f} (0-1 scale)\n"
            f"COVERAGE CONSISTENCY: {cohesion:.3f} (higher = outlets agree)\n"
            f"TOPICS: {topic_str}\n"
            f"TICKERS: {tickers}\n"
            f"PRICE CONTEXT: {price_context or 'unavailable'}"
            f"{update_ctx}"
        )

        return f"{self._SYSTEM_PROMPT}\n\n---\nDATA:\n{data_block}"

    def _parse_response(self, raw):
        """Parse HEADLINE: ... / --- / body from Haiku response."""
        if not raw or not raw.strip():
            return None, None

        lines = raw.strip().splitlines()
        headline = None
        body_start = None

        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.upper().startswith("HEADLINE:") and headline is None:
                headline = stripped[len("HEADLINE:"):].strip().strip('"').strip("'").rstrip(".")
                continue
            if stripped == "---" and headline is not None:
                body_start = i + 1
                break

        if headline and body_start is not None and body_start < len(lines):
            body = "\n".join(lines[body_start:]).strip()
            if body:
                # Sanitize: remove any stray HEADLINE: or --- that Haiku might echo
                body = body.replace("HEADLINE:", "").strip()
                return headline[:120], body[:2000]

        return None, None

    def _template_fallback(self, narrative, trigger_type, price_context):
        """Deterministic fallback when Haiku fails."""
        name = _safe_text(narrative.get("name", "Unknown story"))
        short = _short_name(name)
        tickers = _format_tickers(narrative)
        doc_count = narrative.get("document_count", 0)
        stage = narrative.get("stage", "")

        headline = f"{short} — {doc_count} Sources and {_stage_word(stage).title()}"

        price_line = ""
        if price_context and price_context != "unavailable":
            price_line = f"\n\n{price_context}"

        body = (
            f"{doc_count} independent sources are covering {short} "
            f"and the story is still {_stage_word(stage)}.{price_line}\n\n"
            f"{tickers}"
        )
        return headline[:120], body[:2000]


class DiscordPoster:
    """Posts draft analysis to a Discord channel via webhook for manual X posting."""

    _ALLOWED_WEBHOOK_HOSTS = ("discord.com", "discordapp.com")

    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        self._validated = self._validate_url(webhook_url)

    @classmethod
    def _validate_url(cls, url):
        """Reject non-Discord URLs to prevent SSRF via webhook config."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            host = (parsed.hostname or "").lower()
            return (
                parsed.scheme == "https"
                and any(host == allowed or host.endswith("." + allowed)
                        for allowed in cls._ALLOWED_WEBHOOK_HOSTS)
            )
        except Exception:
            return False

    def _post_json(self, payload):
        """Send JSON to Discord webhook with 429 retry-after handling.
        Returns (ok: bool, draft_id_or_none)."""
        import requests as req
        for attempt in range(2):
            resp = req.post(
                self.webhook_url,
                json=payload,
                timeout=15,
            )
            if resp.ok:
                return True, f"discord-{uuid.uuid4().hex[:8]}"
            if resp.status_code == 429 and attempt == 0:
                retry_after = 5
                try:
                    retry_after = min(float(resp.json().get("retry_after", 5)), 15)
                except Exception:
                    pass
                logger.warning("Discord rate limited (429), retrying in %.1fs", retry_after)
                time.sleep(retry_after)
                continue
            logger.error("Discord post failed: HTTP %d", resp.status_code)
            return False, None
        return False, None

    def post_draft(self, headline, body, narrative, trigger_type):
        """
        Send a structured draft to Discord with copy-paste block + context.
        Returns a synthetic ID on success, None on failure.
        """
        if not self._validated:
            logger.error("Discord webhook URL rejected by SSRF validation")
            return None
        try:
            import requests as req

            # --- Copy-paste block ---
            post_block = f"**{headline}**\n\n{body}"

            # --- Context block ---
            doc_count = narrative.get("document_count", 0)
            velocity = narrative.get("velocity") or 0
            burst = narrative.get("burst_ratio") or 0
            ns_score = narrative.get("ns_score") or 0
            cohesion = narrative.get("cohesion") or 0
            stage = narrative.get("stage", "")
            tickers_raw = parse_tickers(narrative.get("linked_assets"))

            context_lines = [
                f"Trigger: {trigger_type}",
                f"Signal strength: {ns_score:.3f}",
                f"Spread rate: {velocity:.4f}",
                f"Phase: {stage}",
                f"Sources: {doc_count}",
                f"Burst: {burst:.2f}x",
                f"Cohesion: {cohesion:.3f}",
                f"Tickers: {', '.join(tickers_raw[:6])}",
            ]
            context_block = "\n".join(context_lines)

            message = (
                f"**DRAFT POST** -- copy to X\n\n"
                f"{post_block}\n\n"
                f"---\n"
                f"**Your context**\n"
                f"```\n{context_block}\n```"
            )

            ok, draft_id = self._post_json({
                "content": message[:2000],
                "username": "Canary Intel",
            })
            if ok:
                logger.info("Draft sent to Discord: %s", draft_id)
                return draft_id
            return None
        except Exception as exc:
            logger.error("Discord post failed: %s", type(exc).__name__)
            return None

    def post_macro(self, tweet_text, pattern_type, data):
        """Send a macro tweet draft to Discord. Simpler format than articles."""
        if not self._validated:
            logger.error("Discord webhook URL rejected by SSRF validation")
            return None
        try:
            import requests as req
            context_lines = [f"Type: {pattern_type}"]
            for k, v in data.items():
                if k != "priority":
                    context_lines.append(f"{k}: {v}")
            context_block = "\n".join(context_lines)

            message = (
                f"**MACRO TWEET** -- copy to X\n\n"
                f"{tweet_text}\n\n"
                f"_{len(tweet_text)} chars_\n"
                f"---\n"
                f"**Your context**\n"
                f"```\n{context_block}\n```"
            )

            ok, draft_id = self._post_json({
                "content": message[:2000],
                "username": "Canary Intel",
            })
            if ok:
                logger.info("Macro draft sent to Discord: %s", draft_id)
                return draft_id
            return None
        except Exception as exc:
            logger.error("Discord macro failed: %s", type(exc).__name__)
            return None

    # Legacy interface — kept for TypefullyPoster/TwitterPoster compatibility
    def post_tweet(self, text, reply_to=None, quote_tweet_id=None):
        """Fallback: send plain text draft to Discord."""
        if not self._validated:
            logger.error("Discord webhook URL rejected by SSRF validation")
            return None
        try:
            content = f"**Draft tweet** (copy to X):\n```\n{text}\n```"
            ok, draft_id = self._post_json(
                {"content": content[:2000], "username": "Canary Intel"})
            if ok:
                logger.info("Tweet draft sent to Discord: %s", draft_id)
                return draft_id
            return None
        except Exception as exc:
            logger.error("Discord post failed: %s", type(exc).__name__)
            return None

    def post_thread(self, tweets):
        """Send a thread as a single Discord message."""
        if not self._validated:
            logger.error("Discord webhook URL rejected by SSRF validation")
            return [None] * len(tweets)
        try:
            parts = ["**Draft thread** (copy to X as replies):\n"]
            for i, t in enumerate(tweets):
                label = "Main tweet" if i == 0 else f"Reply {i}"
                parts.append(f"**{label}:**\n```\n{t}\n```")
            content = "\n".join(parts)
            ok, draft_id = self._post_json(
                {"content": content[:2000], "username": "Canary Intel"})
            if ok:
                logger.info("Thread draft sent to Discord: %s (%d tweets)",
                            draft_id, len(tweets))
                return [draft_id] * len(tweets)
            return [None] * len(tweets)
        except Exception as exc:
            logger.error("Discord thread failed: %s", type(exc).__name__)
            return [None] * len(tweets)


class TypefullyPoster:
    """Posts to X via Typefully's API. No tweepy or X API credits needed."""

    def __init__(self, api_key, social_set_id):
        self.api_key = api_key
        self.social_set_id = social_set_id
        self.base_url = "https://api.typefully.com/v2"

    def post_tweet(self, text, reply_to=None, quote_tweet_id=None):
        """Post a single tweet via Typefully. Returns draft ID on success, None on failure."""
        try:
            import requests as req
            resp = req.post(
                f"{self.base_url}/social-sets/{self.social_set_id}/drafts",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "platforms": {"x": {"enabled": True, "posts": [{"text": text}]}},
                    "publish_at": "now",
                },
                timeout=15,
            )
            if resp.ok:
                try:
                    draft_id = resp.json().get("id", "typefully-ok")
                except (ValueError, KeyError):
                    draft_id = "typefully-ok"
                logger.info("Tweet scheduled via Typefully: id=%s", draft_id)
                return str(draft_id)
            logger.error("Typefully post failed: HTTP %d", resp.status_code)
            return None
        except Exception as exc:
            logger.error("Typefully post failed: %s", type(exc).__name__)
            return None

    def post_thread(self, tweets):
        """Post a thread as a single Typefully draft. Returns list of IDs."""
        try:
            import requests as req
            posts = [{"text": t} for t in tweets]
            resp = req.post(
                f"{self.base_url}/social-sets/{self.social_set_id}/drafts",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "platforms": {"x": {"enabled": True, "posts": posts}},
                    "publish_at": "now",
                },
                timeout=15,
            )
            if resp.ok:
                try:
                    draft_id = str(resp.json().get("id", "typefully-ok"))
                except (ValueError, KeyError):
                    draft_id = "typefully-ok"
                logger.info("Thread scheduled via Typefully: id=%s (%d tweets)", draft_id, len(tweets))
                return [draft_id] * len(tweets)
            logger.error("Typefully thread failed: HTTP %d", resp.status_code)
            return [None] * len(tweets)
        except Exception as exc:
            logger.error("Typefully thread failed: %s", type(exc).__name__)
            return [None] * len(tweets)


class TwitterPoster:
    """Handles X API v2 interaction via tweepy."""

    def __init__(self, api_key, api_secret, access_token, access_token_secret):
        self.client = None
        try:
            import tweepy
            self.client = tweepy.Client(
                consumer_key=api_key,
                consumer_secret=api_secret,
                access_token=access_token,
                access_token_secret=access_token_secret,
            )
        except ImportError:
            logger.error("tweepy not installed. Run: pip install tweepy")
        except Exception as exc:
            logger.error("Failed to initialize Twitter client: %s",
                         type(exc).__name__)

    def post_tweet(self, text, reply_to=None, quote_tweet_id=None):
        """
        Post a single tweet. Returns tweet ID string on success, None on failure.
        Never raises. Never retries.
        """
        if not self.client:
            logger.warning("Twitter client not initialized, skipping")
            return None
        try:
            kwargs = {"text": text}
            if reply_to:
                kwargs["in_reply_to_tweet_id"] = reply_to
            if quote_tweet_id:
                kwargs["quote_tweet_id"] = quote_tweet_id
            response = self.client.create_tweet(**kwargs)
            tweet_id = response.data.get("id") if response.data else None
            logger.info("Tweet posted: id=%s", tweet_id)
            return str(tweet_id) if tweet_id else None
        except Exception as exc:
            detail = str(exc) or repr(exc)
            if hasattr(exc, "api_messages") and exc.api_messages:
                detail = "; ".join(exc.api_messages)
            elif hasattr(exc, "response") and hasattr(exc.response, "status_code"):
                detail = f"HTTP {exc.response.status_code}: {exc}"
            logger.error("Tweet failed: %s — %s", type(exc).__name__, detail)
            return None

    def post_thread(self, tweets):
        """Post a thread (list of tweets). Returns list of tweet IDs."""
        ids = []
        reply_to = None
        for text in tweets:
            tweet_id = self.post_tweet(text, reply_to=reply_to)
            ids.append(tweet_id)
            if tweet_id:
                reply_to = tweet_id
            else:
                break
        return ids


class BudgetManager:
    """Tracks daily and monthly tweet budget."""

    def __init__(self, repository, daily_limit=35, monthly_limit=0):
        self.repo = repository
        self.daily_limit = daily_limit
        self.monthly_limit = monthly_limit  # 0 = no monthly cap
        self._daily_used = None
        self._monthly_used = None

    def tweets_remaining(self):
        if self._daily_used is None:
            self._daily_used = self.repo.get_tweet_count_today()
        daily_remaining = max(0, self.daily_limit - self._daily_used)
        if self.monthly_limit <= 0:
            return daily_remaining
        if self._monthly_used is None:
            self._monthly_used = self.repo.get_tweet_count_this_month()
        monthly_remaining = max(0, self.monthly_limit - self._monthly_used)
        return min(daily_remaining, monthly_remaining)

    def can_post(self, count=1):
        return self.tweets_remaining() >= count

    def record_posted(self, count=1):
        """Track posts within this cycle without re-querying DB."""
        if self._daily_used is None:
            self._daily_used = self.repo.get_tweet_count_today()
        self._daily_used += count
        if self.monthly_limit > 0:
            if self._monthly_used is None:
                self._monthly_used = self.repo.get_tweet_count_this_month()
            self._monthly_used += count


# ---------------------------------------------------------------------------
# Main entry point - called from pipeline
# ---------------------------------------------------------------------------

DISCORD_MAX_PER_CYCLE = 10
LEGACY_MAX_PER_CYCLE = 3


def dispatch_tweets(repository, settings):
    """
    Retired in revamp phase 3.
    Kept as a compatibility no-op for older callsites.
    """
    logger.info("dispatch_tweets retired: outbound posting is disabled")
    return None


def _dispatch_discord(repository, settings):
    """Discord flow: narrative articles only (new + changes). No macro filler."""
    from llm_client import LlmClient

    poster = DiscordPoster(settings.DISCORD_WEBHOOK_URL)
    llm = LlmClient(settings, repository)
    article_composer = HaikuDraftComposer(llm)

    # Discord gets relaxed detection: no ticker requirement (internal feed,
    # not public X), higher new-per-cycle cap so fresh narratives get through.
    detector = ChangeDetector(
        repository, require_tickers=False, max_new_per_cycle=8)

    active = repository.get_all_active_narratives()
    events = detector.find_tweetable_events(active)

    to_send = events[:DISCORD_MAX_PER_CYCLE]

    attempted = 0
    succeeded = 0

    for narrative, trigger_type, priority in to_send:
        if attempted > 0:
            time.sleep(2)

        nid = narrative["narrative_id"]
        last_tweet = repository.get_last_tweet_for_narrative(nid)
        price_context = _build_price_context(narrative)
        headline, body = article_composer.compose(
            narrative, trigger_type, price_context, last_tweet)
        draft_id = poster.post_draft(
            headline, body, narrative, trigger_type)
        full_text = f"{headline}\n\n{body}"
        _log_tweet(repository, nid, draft_id, full_text,
                   f"discord_{trigger_type}", narrative)

        attempted += 1
        if draft_id:
            succeeded += 1

    logger.info("Discord dispatch: %d/%d succeeded, %d attempted",
                succeeded, len(to_send), attempted)


def _dispatch_legacy(repository, settings, poster, daily_budget, monthly_budget):
    """Legacy flow: template-based tweets via Typefully or direct Twitter."""
    budget = BudgetManager(repository, daily_budget, monthly_budget)
    if not budget.can_post():
        logger.info("Tweet budget exhausted, skipping")
        return

    detector = ChangeDetector(repository)
    composer = TweetComposer()

    active = repository.get_all_active_narratives()
    events = detector.find_tweetable_events(active)

    max_per_cycle = min(LEGACY_MAX_PER_CYCLE, budget.tweets_remaining())
    attempted = 0
    succeeded = 0
    for narrative, trigger_type, priority in events:
        if attempted >= max_per_cycle or not budget.can_post():
            logger.info("Tweet cycle/budget limit reached, stopping")
            break

        if attempted > 0:
            time.sleep(30)

        nid = narrative["narrative_id"]
        last_tweet = repository.get_last_tweet_for_narrative(nid)

        # Quote-tweet if we have an original tweet ID for this narrative
        if (last_tweet
                and last_tweet.get("tweet_id")
                and trigger_type in ("stage_change", "velocity_spike",
                                     "coverage_surge")):
            original = repository.get_original_tweet_for_narrative(nid)
            quote_id = (original or last_tweet).get("tweet_id")
            if quote_id:
                text = composer.compose_quote_update(narrative, trigger_type)
                tweet_id = poster.post_tweet(text, quote_tweet_id=quote_id)
                _log_tweet(repository, nid, tweet_id, text,
                           f"quote_{trigger_type}", narrative, quote_id)
                attempted += 1
                if tweet_id:
                    succeeded += 1
                    budget.record_posted(1)
                continue

        tweets = composer.compose(narrative, trigger_type, last_tweet)
        if len(tweets) == 1:
            tweet_id = poster.post_tweet(tweets[0])
            _log_tweet(repository, nid, tweet_id, tweets[0],
                       trigger_type, narrative)
            attempted += 1
            if tweet_id:
                succeeded += 1
                budget.record_posted(1)
        else:
            if not budget.can_post(len(tweets)):
                continue
            ids = poster.post_thread(tweets)
            for i, (text, tid) in enumerate(zip(tweets, ids)):
                ttype = "thread_head" if i == 0 else "thread_reply"
                parent = ids[0] if i > 0 else None
                _log_tweet(repository, nid, tid, text, ttype,
                           narrative, parent)
            actually_posted = len([x for x in ids if x is not None])
            attempted += 1
            succeeded += actually_posted
            budget.record_posted(actually_posted)

    logger.info("Tweet dispatch: %d succeeded, %d attempted from %d events",
                succeeded, attempted, len(events))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log_tweet(repository, narrative_id, tweet_id, text, tweet_type,
               narrative, parent_tweet_id=None):
    try:
        metrics = {
            "velocity": narrative.get("velocity"),
            "stage": narrative.get("stage"),
            "document_count": narrative.get("document_count"),
            "burst_ratio": narrative.get("burst_ratio"),
        }
        repository.insert_tweet_log({
            "id": str(uuid.uuid4()),
            "narrative_id": narrative_id,
            "tweet_id": tweet_id,
            "tweet_text": text,
            "tweet_type": tweet_type,
            "parent_tweet_id": parent_tweet_id,
            "metrics_snapshot": json.dumps(metrics),
            "posted_at": datetime.now(timezone.utc).isoformat(),
            "status": "posted" if tweet_id else "failed",
        })
    except Exception as exc:
        logger.error("Failed to log tweet for %s: %s", narrative_id,
                     type(exc).__name__)


def _macro_dedup_id(pattern_type, data):
    """Stable dedup key for macro pattern tracking.

    Returns a string like 'macro_sector_convergence_technology' so each
    unique sector/ticker pattern has its own cooldown window in tweet_log.
    """
    if pattern_type in ("sector_convergence", "stage_clustering",
                        "sector_accelerating", "sector_decelerating"):
        key = (data.get("sector") or "unknown").lower().replace(" ", "_")
        return f"macro_{pattern_type}_{key}"
    if pattern_type == "ticker_convergence":
        key = (data.get("ticker") or "unknown").upper()
        return f"macro_{pattern_type}_{key}"
    return f"macro_{pattern_type}"


def _log_macro_tweet(repository, tweet_id, text, pattern_type, data):
    """Log a macro tweet to tweet_log with specific dedup key as narrative_id."""
    try:
        repository.insert_tweet_log({
            "id": str(uuid.uuid4()),
            "narrative_id": _macro_dedup_id(pattern_type, data),
            "tweet_id": tweet_id,
            "tweet_text": text,
            "tweet_type": f"macro_{pattern_type}",
            "parent_tweet_id": None,
            "metrics_snapshot": json.dumps(data, default=str),
            "posted_at": datetime.now(timezone.utc).isoformat(),
            "status": "posted" if tweet_id else "failed",
        })
    except Exception as exc:
        logger.error("Failed to log macro tweet: %s", type(exc).__name__)


def parse_tickers(linked_assets):
    """Extract ticker symbols from linked_assets JSON string or list."""
    if not linked_assets:
        return []
    try:
        assets = (json.loads(linked_assets)
                  if isinstance(linked_assets, str) else linked_assets)
    except (json.JSONDecodeError, TypeError, ValueError):
        return []
    if not isinstance(assets, list):
        return []
    tickers = []
    for a in assets:
        if isinstance(a, dict):
            t = a.get("ticker", "")
        elif isinstance(a, str):
            t = a
        else:
            continue
        if t and not t.startswith("TOPIC:"):
            tickers.append(t)
    return tickers


def parse_tickers_raw(cashtag_str):
    """Parse cashtag string like '$NVDA $AMD' back to list."""
    if not cashtag_str:
        return []
    return [t.lstrip("$") for t in cashtag_str.split() if t.startswith("$")]


def _format_tickers(narrative):
    """Format linked_assets as cashtag string, max 4."""
    tickers = parse_tickers(narrative.get("linked_assets"))
    return " ".join(f"${t}" for t in tickers[:4])


def _short_name(name):
    """Shorten narrative name for tweet context."""
    if not name:
        return "this story"
    if len(name) <= 60:
        return name
    # Try splitting at separators, but only if the left half is substantial
    for sep in [" and ", ": ", " - "]:
        idx = name.find(sep)
        if idx >= 20:
            return name[:idx]
    # Fall back to word-boundary truncation
    truncated = name[:57]
    last_space = truncated.rfind(" ")
    if last_space > 30:
        return truncated[:last_space]
    return truncated + "..."


def _stage_word(stage):
    return {
        "Emerging": "early",
        "Growing": "building",
        "Mature": "established",
        "Declining": "fading",
        "Dormant": "quiet",
    }.get(stage, "developing")


def _truncate(text):
    """Ensure tweet fits 280 chars. Breaks at word boundary."""
    if len(text) <= MAX_TWEET_LENGTH:
        return text
    cut = text[:277]
    last_space = cut.rfind(" ")
    if last_space > 200:
        return cut[:last_space] + "..."
    return cut + "..."


def _hours_since(iso_timestamp):
    if not iso_timestamp:
        return 999
    try:
        dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except (ValueError, TypeError):
        return 999


def _parse_linked_assets_full(linked_assets):
    """Parse linked_assets JSON into list of dicts with ticker + asset_name."""
    if not linked_assets:
        return []
    try:
        assets = (json.loads(linked_assets)
                  if isinstance(linked_assets, str) else linked_assets)
    except (json.JSONDecodeError, TypeError, ValueError):
        return []
    if not isinstance(assets, list):
        return []
    result = []
    for a in assets:
        if isinstance(a, dict) and a.get("ticker") and not a["ticker"].startswith("TOPIC:"):
            result.append(a)
        elif isinstance(a, str) and a and not a.startswith("TOPIC:"):
            result.append({"ticker": a, "asset_name": a})
    return result


def _safe_text(text):
    """Strip control characters, prompt delimiters, and limit length for LLM prompt safety."""
    if not text:
        return ""
    # Remove characters that could break prompt formatting
    cleaned = "".join(c for c in str(text) if c.isprintable() or c in ("\n", " "))
    # Strip prompt-injection markers that could hijack LLM output parsing
    for marker in ("HEADLINE:", "SIGNAL_JSON:", "---"):
        cleaned = cleaned.replace(marker, "")
    return cleaned[:200]


# ---------------------------------------------------------------------------
# Price-claim extraction and validation
# ---------------------------------------------------------------------------

_PRICE_CLAIM_RE = re.compile(
    r'\$([A-Z]{1,5}(?:-[A-Z]{1,5})?)'       # $TICKER or $TICKER-USD
    r'\s+'
    r'(up|down)'                              # direction
    r'\s+'
    r'(?:roughly |around |nearly |about )?'   # optional hedge word
    r'(\d+(?:\.\d+)?)\s*%',                   # percentage
    re.IGNORECASE,
)

_PRICE_TAG_RE = re.compile(
    r'\(\$(\d+(?:,\d{3})*(?:\.\d{1,2})?)\)',  # ($385.95) or ($9.50)
)


def _parse_price_claims(text):
    """
    Extract structured price claims from a text string.
    Returns list of dicts: [{"ticker": "TSLA", "direction": "up",
                             "pct": 4.2, "price": 385.95}, ...]
    Works on both the source price_context string and generated body text.
    """
    if not text:
        return []
    claims = []
    for m in _PRICE_CLAIM_RE.finditer(text):
        ticker = m.group(1).upper()
        direction = m.group(2).lower()
        pct = float(m.group(3))

        # Look for a dollar price tag in the ~60 chars after this match
        remainder = text[m.end():m.end() + 60]
        price = None
        pm = _PRICE_TAG_RE.search(remainder)
        if pm:
            price = float(pm.group(1).replace(",", ""))

        claims.append({
            "ticker": ticker,
            "direction": direction,
            "pct": pct,
            "price": price,
        })
    return claims


_PRICE_PCT_TOLERANCE = 1.5     # max absolute percentage-point difference
_PRICE_DOLLAR_TOLERANCE = 0.50  # max dollar difference


def _validate_price_accuracy(generated_text, price_context):
    """
    Compare price claims in generated_text against source price_context.
    Returns (is_valid, reasons) where reasons is a list of failure strings.

    Rules:
    1. Every ticker with a price claim in generated_text must exist in source.
    2. Direction (up/down) must match exactly.
    3. Percentage within _PRICE_PCT_TOLERANCE.
    4. Dollar price (if both present) within _PRICE_DOLLAR_TOLERANCE.
    """
    if not price_context or price_context == "unavailable":
        return True, []

    source_claims = _parse_price_claims(price_context)
    if not source_claims:
        return True, []

    source_by_ticker = {}
    for sc in source_claims:
        if sc["ticker"] not in source_by_ticker:
            source_by_ticker[sc["ticker"]] = sc

    generated_claims = _parse_price_claims(generated_text)
    if not generated_claims:
        return True, []

    reasons = []
    for gc in generated_claims:
        ticker = gc["ticker"]

        if ticker not in source_by_ticker:
            reasons.append(f"${ticker} price claim not in source data")
            continue

        src = source_by_ticker[ticker]

        if gc["direction"] != src["direction"]:
            reasons.append(
                f"${ticker} direction mismatch: "
                f"generated '{gc['direction']}' vs source '{src['direction']}'"
            )
            continue

        pct_diff = abs(gc["pct"] - src["pct"])
        if pct_diff > _PRICE_PCT_TOLERANCE:
            reasons.append(
                f"${ticker} pct divergence: "
                f"generated {gc['pct']}% vs source {src['pct']}% "
                f"(diff {pct_diff:.1f}, tolerance {_PRICE_PCT_TOLERANCE})"
            )

        if gc["price"] is not None and src["price"] is not None:
            price_diff = abs(gc["price"] - src["price"])
            if price_diff > _PRICE_DOLLAR_TOLERANCE:
                reasons.append(
                    f"${ticker} price divergence: "
                    f"${gc['price']:.2f} vs ${src['price']:.2f} "
                    f"(diff ${price_diff:.2f})"
                )

    return len(reasons) == 0, reasons


_EDITORIAL_BLACKLIST = [
    "repositioning", "pricing in", "priced in", "prices in",
    "accumulating", "accumulation",
    "smart money", "dumb money",
    "institutional buying", "institutional selling",
    "institutional investors are",
    "insiders", "insider buying", "insider selling",
    "whales", "whale",
    "big money", "large players", "major players",
    "hedge funds are", "funds are loading", "funds are dumping",
    "shorts are", "short squeeze incoming",
    "capitulation", "distribution phase", "accumulation phase",
]


def _check_editorial_claims(text):
    """
    Scan text for market-commentary claims that cannot be derived
    from the input data. Returns (is_clean, flagged_phrases).
    """
    if not text:
        return True, []
    lower = text.lower()
    flagged = [phrase for phrase in _EDITORIAL_BLACKLIST if phrase in lower]
    return len(flagged) == 0, flagged


def _build_price_context(narrative):
    """
    Fetch recent price data for linked tickers and format as plain English.
    Uses 5 trading day close-to-close change (standard '5D' metric).
    Returns a string like '$TSLA down 4.2% over 5 days ($385.95)'.
    Never raises.
    """
    tickers = parse_tickers(narrative.get("linked_assets"))
    if not tickers:
        return "unavailable"

    try:
        from stock_data import get_price_history
    except ImportError:
        logger.debug("stock_data not available for price context")
        return "unavailable"

    parts = []
    for ticker in tickers[:4]:
        try:
            if ticker.startswith("TOPIC:"):
                continue
            # Fetch 14 calendar days to ensure we have 5+ trading days
            history = get_price_history(ticker, days=14)
            if len(history) < 2:
                continue
            latest = history[-1]
            # Pick the close from exactly 5 trading days ago
            baseline_idx = max(0, len(history) - 6)
            baseline = history[baseline_idx]
            if baseline["close"] > 0:
                change = ((latest["close"] - baseline["close"])
                          / baseline["close"]) * 100
                direction = "up" if change >= 0 else "down"
                parts.append(
                    f"${ticker} {direction} {abs(change):.1f}% over 5 days "
                    f"(${latest['close']:.2f})"
                )
        except Exception:
            continue

    return "; ".join(parts) if parts else "unavailable"
