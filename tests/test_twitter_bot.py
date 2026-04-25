"""
Twitter Bot (Canary Intel) Tests

Unit:
  TW-U1: parse_tickers extracts tickers from dict-style linked_assets
  TW-U2: parse_tickers extracts tickers from string-style linked_assets
  TW-U3: parse_tickers skips TOPIC: prefixed entries
  TW-U4: parse_tickers returns empty list for None/empty input
  TW-U5: _format_tickers caps at 4 cashtags
  TW-U6: _short_name truncates names over 60 chars
  TW-U7: _truncate enforces 280 char limit
  TW-U8: _stage_word maps all stages
  TW-U9: No emojis in any composed tweet
  TW-U10: No em dashes in any composed tweet
  TW-U11: Composed tweets use cashtags not hashtags

Composer:
  TW-C1: new_narrative returns non-empty list
  TW-C2: velocity_spike returns non-empty list
  TW-C3: stage_change returns non-empty list
  TW-C4: coverage_surge returns non-empty list
  TW-C5: compose_quote_update returns string under 280 chars
  TW-C6: compose_thread returns multiple tweets with thread- in head
  TW-C7: new_narrative with high doc_count + 3 tickers returns thread

ChangeDetector:
  TW-D1: New narrative (never tweeted) detected as tweetable
  TW-D2: Narrative with no linked tickers is skipped
  TW-D3: Narrative with doc_count < 8 is skipped
  TW-D4: Stage change detected when stage differs from last tweet
  TW-D5: 24h cooldown blocks velocity_spike re-trigger
  TW-D6: Stage change bypasses 24h cooldown
  TW-D7: 7-day rolling cap blocks after 3 posts
  TW-D8: 7-day rolling cap blocks even stage_change
  TW-D9: New narrative unaffected by cooldown/cap
  TW-D10: Exactly at 24h boundary passes cooldown

BudgetManager:
  TW-B1: can_post returns True when budget available
  TW-B2: can_post returns False when budget exhausted

Repository:
  TW-R1: tweet_log table created by migrate
  TW-R2: insert_tweet_log and get_last_tweet_for_narrative round-trip
  TW-R3: get_original_tweet_for_narrative returns first successful tweet
  TW-R4: get_tweet_count_today counts today's tweets
  TW-R5: get_tweet_count_for_narrative_since counts correctly

Settings:
  TW-S1: TWITTER_API_KEY removed from settings
  TW-S2: TWITTER_ENABLED removed from settings
  TW-S3: TYPEFULLY_ENABLED removed from settings

Macro Dedup:
  TW-MD1: sector_convergence dedup key includes sector
  TW-MD2: ticker_convergence dedup key includes ticker
  TW-MD3: sector_convergence dedup key with missing sector
  TW-MD4: stage_clustering dedup key includes sector
  TW-MD5: high_volume_new dedup key is generic
  TW-MD6: different sectors produce different dedup keys
  TW-MD7: MACRO_COOLDOWN_HOURS is 48
  TW-MD8a: no prior macro post returns None
  TW-MD8b: after insert, get_last_tweet returns the entry
  TW-MD8c: different sector key returns None (independent cooldowns)

Raised Macro Thresholds:
  TW-TH1: 2 narratives in sector no longer fires sector_convergence
  TW-TH2: 3 narratives in sector fires sector_convergence
  TW-TH3: low pressure_score blocked by ticker_convergence
  TW-TH4: high pressure_score passes ticker_convergence
  TW-TH5: 2 narratives no longer fires sector_direction

Template Fallback Quality:
  TW-TQ1: sector_convergence template drops 'None reference each other'
  TW-TQ2: sector_convergence template mentions sector name
  TW-TQ3: sector_convergence template under 280 chars
  TW-TQ4: ticker_convergence template mentions direction
  TW-TQ5: all template fallbacks fit 280 chars
  TW-TQ6: no template uses 'pointing at' phrasing
"""

import atexit
import json
import os
import re
import sys
import shutil
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from twitter_bot import (
    ChangeDetector,
    TweetComposer,
    HaikuDraftComposer,
    MacroTweetComposer,
    MacroPatternDetector,
    DiscordPoster,
    BudgetManager,
    parse_tickers,
    _format_tickers,
    _short_name,
    _truncate,
    _stage_word,
    _safe_text,
    _build_price_context,
    _parse_price_claims,
    _validate_price_accuracy,
    _check_editorial_claims,
    _macro_dedup_id,
    MAX_TWEET_LENGTH,
    DISCORD_MAX_PER_CYCLE,
    MACRO_COOLDOWN_HOURS,
)
from repository import SqliteRepository

_tmp_dir = Path(_ROOT) / f".tmp_test_twitter_bot_{uuid.uuid4().hex}"
_tmp_dir.mkdir(parents=False, exist_ok=False)
atexit.register(shutil.rmtree, _tmp_dir, ignore_errors=True)

# ---------------------------------------------------------------------------
_results = []


def S(section: str):
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = ""):
    _results.append((name, condition))
    marker = "\u2713" if condition else "\u2717"
    msg = f"  [{marker}] {name}"
    if details and not condition:
        msg += f"\n      details: {details}"
    print(msg)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_narrative(**overrides):
    base = {
        "narrative_id": str(uuid.uuid4()),
        "name": "Oil Supply Disruption Drives Global Market Volatility",
        "stage": "Mature",
        "velocity": 0.14,
        "document_count": 120,
        "burst_ratio": 1.2,
        "linked_assets": json.dumps([
            {"ticker": "USO", "asset_name": "US Oil Fund", "similarity_score": 0.85},
            {"ticker": "XLE", "asset_name": "Energy Select", "similarity_score": 0.72},
        ]),
        "suppressed": 0,
    }
    base.update(overrides)
    return base


def _make_tweet_log(narrative_id, **overrides):
    base = {
        "id": str(uuid.uuid4()),
        "narrative_id": narrative_id,
        "tweet_id": "12345678",
        "tweet_text": "test tweet",
        "tweet_type": "new_narrative",
        "parent_tweet_id": None,
        "metrics_snapshot": json.dumps({
            "velocity": 0.10,
            "stage": "Growing",
            "document_count": 50,
            "burst_ratio": 1.0,
        }),
        "posted_at": datetime.now(timezone.utc).isoformat(),
        "status": "posted",
    }
    base.update(overrides)
    return base


_repo_counter = 0
_seed_db = Path(_ROOT) / "data" / "narrative_engine.db"

def _clear_seed_data(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        for table in tables:
            escaped = table.replace('"', '""')
            conn.execute(f'DELETE FROM "{escaped}"')
        conn.commit()


def _get_test_repo():
    global _repo_counter
    _repo_counter += 1
    db_path = str(Path(_tmp_dir) / f"test_tw_{_repo_counter}.db")
    shutil.copy2(_seed_db, db_path)
    _clear_seed_data(db_path)
    repo = SqliteRepository(db_path)
    repo.migrate()
    return repo


EMOJI_PATTERN = re.compile(
    "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U000024C2-\U0001F251"
    "\U0001f900-\U0001f9FF\U0001fa00-\U0001fa6f\U0001fa70-\U0001faff]+",
    flags=re.UNICODE,
)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

S("Unit: parse_tickers and formatting")

# TW-U1
dict_assets = json.dumps([
    {"ticker": "NVDA", "asset_name": "NVIDIA"},
    {"ticker": "AMD", "asset_name": "AMD"},
])
result = parse_tickers(dict_assets)
T("TW-U1: parse_tickers dict-style",
  result == ["NVDA", "AMD"], f"got {result}")

# TW-U2
str_assets = json.dumps(["AAPL", "MSFT"])
result = parse_tickers(str_assets)
T("TW-U2: parse_tickers string-style",
  result == ["AAPL", "MSFT"], f"got {result}")

# TW-U3
topic_assets = json.dumps([
    {"ticker": "TOPIC:energy"},
    {"ticker": "USO"},
])
result = parse_tickers(topic_assets)
T("TW-U3: parse_tickers skips TOPIC:",
  result == ["USO"], f"got {result}")

# TW-U4
T("TW-U4: parse_tickers None input", parse_tickers(None) == [])
T("TW-U4b: parse_tickers empty string", parse_tickers("") == [])

# TW-U5
many_tickers = _make_narrative(linked_assets=json.dumps([
    {"ticker": t} for t in ["A", "B", "C", "D", "E"]
]))
formatted = _format_tickers(many_tickers)
T("TW-U5: _format_tickers caps at 4",
  formatted.count("$") == 4, f"got '{formatted}'")

# TW-U6
T("TW-U6: _short_name truncates long names",
  len(_short_name("A" * 80)) <= 63)

# TW-U7
long_text = "x" * 300
T("TW-U7: _truncate enforces 280 chars",
  len(_truncate(long_text)) <= MAX_TWEET_LENGTH)

# TW-U8
for stage in ["Emerging", "Growing", "Mature", "Declining", "Dormant"]:
    word = _stage_word(stage)
    T(f"TW-U8: _stage_word({stage})",
      word and isinstance(word, str), f"got {word}")

# TW-U9, TW-U10, TW-U11: Voice rules across all trigger types
S("Unit: Voice rules (no emojis, no em dashes, cashtags)")
composer = TweetComposer()
narrative = _make_narrative()

all_tweets = []
for trigger in ["new_narrative", "velocity_spike", "stage_change", "coverage_surge"]:
    last = _make_tweet_log(narrative["narrative_id"]) if trigger != "new_narrative" else None
    tweets = composer.compose(narrative, trigger, last)
    all_tweets.extend(tweets)

quote = composer.compose_quote_update(narrative, "stage_change")
all_tweets.append(quote)

has_emoji = any(EMOJI_PATTERN.search(t) for t in all_tweets)
T("TW-U9: No emojis in composed tweets", not has_emoji,
  f"found emoji in: {[t for t in all_tweets if EMOJI_PATTERN.search(t)]}")

has_em_dash = any("\u2014" in t or "\u2013" in t for t in all_tweets)
T("TW-U10: No em dashes in composed tweets", not has_em_dash)

has_hashtag = any(re.search(r"(?<!\$)#\w+", t) for t in all_tweets)
has_cashtag = any("$" in t for t in all_tweets)
T("TW-U11a: No hashtags in tweets", not has_hashtag)
T("TW-U11b: Cashtags present in tweets", has_cashtag)

# ---------------------------------------------------------------------------
# Composer tests
# ---------------------------------------------------------------------------

S("Composer: trigger types")

# TW-C1
tweets = composer.compose(narrative, "new_narrative")
T("TW-C1: new_narrative returns non-empty",
  len(tweets) >= 1 and all(len(t) <= MAX_TWEET_LENGTH for t in tweets))

# TW-C2
spike_narrative = _make_narrative(burst_ratio=4.0)
tweets = composer.compose(spike_narrative, "velocity_spike")
T("TW-C2: velocity_spike returns non-empty",
  len(tweets) >= 1 and all(len(t) <= MAX_TWEET_LENGTH for t in tweets))

# TW-C3
last = _make_tweet_log(narrative["narrative_id"])
tweets = composer.compose(narrative, "stage_change", last)
T("TW-C3: stage_change returns non-empty",
  len(tweets) >= 1 and all(len(t) <= MAX_TWEET_LENGTH for t in tweets))

# TW-C4
tweets = composer.compose(narrative, "coverage_surge", last)
T("TW-C4: coverage_surge returns non-empty",
  len(tweets) >= 1 and all(len(t) <= MAX_TWEET_LENGTH for t in tweets))

# TW-C5
qt = composer.compose_quote_update(narrative, "velocity_spike")
T("TW-C5: quote_update under 280 chars",
  isinstance(qt, str) and len(qt) <= MAX_TWEET_LENGTH)

# TW-C6
narratives = [_make_narrative(name=f"Story {i}") for i in range(3)]
thread = composer.compose_thread(narratives, "semiconductors", "$NVDA $AMD")
T("TW-C6a: thread returns multiple tweets", len(thread) >= 2)
T("TW-C6b: thread head contains 'thread-'",
  "thread-" in thread[0] if thread else False)

# TW-C7
big_narrative = _make_narrative(
    document_count=80,
    linked_assets=json.dumps([
        {"ticker": t} for t in ["NVDA", "AMD", "INTC", "TSM"]
    ]),
)
tweets = composer.compose(big_narrative, "new_narrative")
T("TW-C7: high doc + 3 tickers creates thread",
  len(tweets) >= 2, f"got {len(tweets)} tweets")

# ---------------------------------------------------------------------------
# ChangeDetector tests
# ---------------------------------------------------------------------------

S("ChangeDetector")

repo = _get_test_repo()

# TW-D1: New narrative detected
det = ChangeDetector(repo)
n1 = _make_narrative()
events = det.find_tweetable_events([n1])
T("TW-D1: new narrative is tweetable",
  len(events) == 1 and events[0][1] == "new_narrative")

# TW-D2: No linked tickers
n2 = _make_narrative(linked_assets="[]")
events = det.find_tweetable_events([n2])
T("TW-D2: no tickers = skipped", len(events) == 0)

# TW-D3: Low doc count
n3 = _make_narrative(document_count=3)
events = det.find_tweetable_events([n3])
T("TW-D3: low doc_count = skipped", len(events) == 0)

# TW-D4: Stage change
nid = str(uuid.uuid4())
n4 = _make_narrative(narrative_id=nid, stage="Mature", velocity=0.12,
                     document_count=50)
repo.insert_tweet_log(_make_tweet_log(nid))  # last tweet has stage=Growing
events = det.find_tweetable_events([n4])
T("TW-D4: stage change detected",
  len(events) == 1 and events[0][1] == "stage_change",
  f"got {[(e[1], e[2]) for e in events]}")

# TW-D5: 24h cooldown blocks velocity_spike re-trigger
nid_d5 = str(uuid.uuid4())
n_d5 = _make_narrative(narrative_id=nid_d5, stage="Growing", velocity=0.20,
                       document_count=50)
repo.insert_tweet_log(_make_tweet_log(nid_d5, posted_at=(
    datetime.now(timezone.utc) - timedelta(hours=4)).isoformat(),
    metrics_snapshot=json.dumps({
        "velocity": 0.10, "stage": "Growing",
        "document_count": 40, "burst_ratio": 1.0,
    })))
events = det.find_tweetable_events([n_d5])
T("TW-D5: 24h cooldown blocks velocity_spike",
  len(events) == 0, f"got {[(e[1], e[2]) for e in events]}")

# TW-D6: stage_change bypasses 24h cooldown
nid_d6 = str(uuid.uuid4())
n_d6 = _make_narrative(narrative_id=nid_d6, stage="Mature", velocity=0.12,
                       document_count=50)
repo.insert_tweet_log(_make_tweet_log(nid_d6, posted_at=(
    datetime.now(timezone.utc) - timedelta(hours=4)).isoformat(),
    metrics_snapshot=json.dumps({
        "velocity": 0.10, "stage": "Growing",
        "document_count": 40, "burst_ratio": 1.0,
    })))
events = det.find_tweetable_events([n_d6])
T("TW-D6: stage_change bypasses 24h cooldown",
  len(events) == 1 and events[0][1] == "stage_change",
  f"got {[(e[1], e[2]) for e in events]}")

# TW-D7: 7-day cap blocks after 3 posts
nid_d7 = str(uuid.uuid4())
n_d7 = _make_narrative(narrative_id=nid_d7, stage="Growing", velocity=0.20,
                       document_count=50)
for i in range(3):
    repo.insert_tweet_log(_make_tweet_log(nid_d7, posted_at=(
        datetime.now(timezone.utc) - timedelta(days=6, hours=i)).isoformat(),
        metrics_snapshot=json.dumps({
            "velocity": 0.10, "stage": "Growing",
            "document_count": 40, "burst_ratio": 1.0,
        })))
events = det.find_tweetable_events([n_d7])
T("TW-D7: 7-day cap blocks after 3 posts",
  len(events) == 0, f"got {[(e[1], e[2]) for e in events]}")

# TW-D8: 7-day cap blocks even stage_change (hard cap)
nid_d8 = str(uuid.uuid4())
n_d8 = _make_narrative(narrative_id=nid_d8, stage="Mature", velocity=0.12,
                       document_count=50)
for i in range(3):
    repo.insert_tweet_log(_make_tweet_log(nid_d8, posted_at=(
        datetime.now(timezone.utc) - timedelta(days=5, hours=i)).isoformat(),
        metrics_snapshot=json.dumps({
            "velocity": 0.10, "stage": "Growing",
            "document_count": 40, "burst_ratio": 1.0,
        })))
events = det.find_tweetable_events([n_d8])
T("TW-D8: 7-day cap blocks even stage_change",
  len(events) == 0, f"got {[(e[1], e[2]) for e in events]}")

# TW-D9: new narrative unaffected by cooldown/cap
nid_d9 = str(uuid.uuid4())
n_d9 = _make_narrative(narrative_id=nid_d9, stage="Growing", velocity=0.15,
                       document_count=50)
events = det.find_tweetable_events([n_d9])
T("TW-D9: new narrative unaffected by cooldown/cap",
  len(events) == 1 and events[0][1] == "new_narrative")

# TW-D10: exactly at 24h boundary passes cooldown
nid_d10 = str(uuid.uuid4())
n_d10 = _make_narrative(narrative_id=nid_d10, stage="Growing", velocity=0.20,
                        document_count=50)
repo.insert_tweet_log(_make_tweet_log(nid_d10, posted_at=(
    datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
    metrics_snapshot=json.dumps({
        "velocity": 0.10, "stage": "Growing",
        "document_count": 40, "burst_ratio": 1.0,
    })))
events = det.find_tweetable_events([n_d10])
T("TW-D10: exactly at 24h boundary passes cooldown",
  len(events) == 1 and events[0][1] == "velocity_spike",
  f"got {[(e[1], e[2]) for e in events]}")

# ---------------------------------------------------------------------------
# BudgetManager tests
# ---------------------------------------------------------------------------

S("BudgetManager")

repo2 = _get_test_repo()

# TW-B1
bm = BudgetManager(repo2, daily_limit=35)
T("TW-B1: can_post when empty", bm.can_post())

# TW-B2
for i in range(35):
    repo2.insert_tweet_log({
        "id": str(uuid.uuid4()),
        "narrative_id": "test",
        "tweet_id": str(i),
        "tweet_text": "t",
        "tweet_type": "test",
        "posted_at": datetime.now(timezone.utc).isoformat(),
        "status": "posted",
    })
bm2 = BudgetManager(repo2, daily_limit=35)
T("TW-B2: can_post False at limit", not bm2.can_post())

# ---------------------------------------------------------------------------
# Repository tweet_log tests
# ---------------------------------------------------------------------------

S("Repository: tweet_log")

repo3 = _get_test_repo()

# TW-R1: table exists
try:
    repo3.get_tweet_count_today()
    T("TW-R1: tweet_log table exists", True)
except Exception as e:
    T("TW-R1: tweet_log table exists", False, str(e))

# TW-R2: round-trip
nid = str(uuid.uuid4())
entry = _make_tweet_log(nid, tweet_text="hello world")
repo3.insert_tweet_log(entry)
got = repo3.get_last_tweet_for_narrative(nid)
T("TW-R2: insert/get round-trip",
  got is not None and got["tweet_text"] == "hello world")

# TW-R3: original tweet
nid2 = str(uuid.uuid4())
repo3.insert_tweet_log(_make_tweet_log(
    nid2, tweet_type="new_narrative", tweet_id="111",
    posted_at="2026-03-24T01:00:00+00:00"))
repo3.insert_tweet_log(_make_tweet_log(
    nid2, tweet_type="quote_stage_change", tweet_id="222",
    posted_at="2026-03-24T05:00:00+00:00"))
orig = repo3.get_original_tweet_for_narrative(nid2)
T("TW-R3: get_original returns first tweet",
  orig is not None and orig["tweet_id"] == "111",
  f"got tweet_id={orig.get('tweet_id') if orig else None}")

# TW-R4: count today
count = repo3.get_tweet_count_today()
T("TW-R4: get_tweet_count_today", count >= 1, f"got {count}")

# TW-R5: get_tweet_count_for_narrative_since
nid_r5 = str(uuid.uuid4())
for offset_hours in [2, 10, 30]:
    repo3.insert_tweet_log(_make_tweet_log(
        nid_r5, posted_at=(
            datetime.now(timezone.utc) - timedelta(hours=offset_hours)
        ).isoformat()))
since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
count_r5 = repo3.get_tweet_count_for_narrative_since(nid_r5, since_24h)
T("TW-R5: get_tweet_count_for_narrative_since",
  count_r5 == 2, f"expected 2, got {count_r5}")

# ---------------------------------------------------------------------------
# Audit: edge cases and adversarial inputs
# ---------------------------------------------------------------------------

S("Audit: edge cases")

# TW-A1: _short_name with early separator should not produce tiny string
short = _short_name("AI and Machine Learning Disruption in Healthcare and Biotech Sectors")
T("TW-A1: _short_name early separator",
  len(short) >= 15, f"got '{short}' ({len(short)} chars)")

# TW-A2: _short_name with no separators, long name
short2 = _short_name("A" * 80)
T("TW-A2: _short_name no separators",
  len(short2) <= 63 and len(short2) > 0)

# TW-A3: _truncate breaks at word boundary
trunc = _truncate("word " * 60)  # 300 chars
T("TW-A3: _truncate word boundary",
  len(trunc) <= MAX_TWEET_LENGTH and not trunc.endswith(" ..."),
  f"got: ...{trunc[-20:]}")

# TW-A4: zero velocity -> zero velocity, no division by zero
det_a = ChangeDetector(_get_test_repo())
n_zero = _make_narrative(velocity=0.0)
try:
    events = det_a.find_tweetable_events([n_zero])
    T("TW-A4: zero velocity no crash", True)
except ZeroDivisionError:
    T("TW-A4: zero velocity no crash", False, "ZeroDivisionError")

# TW-A5: narrative with None velocity
n_none_vel = _make_narrative(velocity=None)
try:
    events = det_a.find_tweetable_events([n_none_vel])
    T("TW-A5: None velocity no crash", True)
except Exception as e:
    T("TW-A5: None velocity no crash", False, str(e))

# TW-A6: narrative with None document_count
n_none_doc = _make_narrative(document_count=None)
events = det_a.find_tweetable_events([n_none_doc])
T("TW-A6: None doc_count skipped", len(events) == 0)

# TW-A7: malicious narrative name in tweet
evil_name = 'BUY $SCAM NOW <script>alert("xss")</script>'
evil_n = _make_narrative(name=evil_name, document_count=20)
tweets = TweetComposer().compose(evil_n, "new_narrative")
T("TW-A7: malicious name passes through (no injection vector in plaintext)",
  len(tweets) >= 1 and all(len(t) <= MAX_TWEET_LENGTH for t in tweets))

# TW-A8: empty linked_assets edge cases
for bad_input in ["null", "not json", "{}", "[[]]"]:
    result = parse_tickers(bad_input)
    T(f"TW-A8: parse_tickers({bad_input!r}) no crash",
      isinstance(result, list))

# TW-A9: compose all template paths for voice violations
S("Audit: exhaustive voice check")
comp = TweetComposer()
all_outputs = []
for stage in ["Emerging", "Growing", "Mature", "Declining"]:
    for trigger in ["new_narrative", "velocity_spike", "stage_change", "coverage_surge"]:
        n = _make_narrative(
            stage=stage, burst_ratio=4.0, document_count=120)
        last = _make_tweet_log(n["narrative_id"])
        tweets = comp.compose(n, trigger, last)
        all_outputs.extend(tweets)
    for qt in ["stage_change", "velocity_spike", "coverage_surge"]:
        n = _make_narrative(stage=stage)
        all_outputs.append(comp.compose_quote_update(n, qt))

voice_fail = False
for t in all_outputs:
    if "\u2014" in t or "\u2013" in t:
        voice_fail = True
    if EMOJI_PATTERN.search(t):
        voice_fail = True
    if len(t) > MAX_TWEET_LENGTH:
        voice_fail = True
T("TW-A9: all template variants pass voice check",
  not voice_fail and len(all_outputs) > 20,
  f"checked {len(all_outputs)} outputs, fail={voice_fail}")

# TW-A10: BudgetManager.record_posted tracks correctly
repo_a10 = _get_test_repo()
bm_a10 = BudgetManager(repo_a10, daily_limit=5)
T("TW-A10a: initial remaining=5", bm_a10.tweets_remaining() == 5)
bm_a10.record_posted(3)
T("TW-A10b: after record_posted(3) remaining=2",
  bm_a10.tweets_remaining() == 2)
bm_a10.record_posted(2)
T("TW-A10c: after record_posted(2) can_post=False",
  not bm_a10.can_post())

# ---------------------------------------------------------------------------
# HaikuDraftComposer tests
# ---------------------------------------------------------------------------

S("HaikuDraftComposer")


class _FakeLlm:
    """Fake LLM client for testing. Returns canned responses."""
    def __init__(self, response=""):
        self._response = response

    def call_haiku(self, task_type, narrative_id, prompt, max_tokens=None):
        return self._response


# TW-H1: Good Haiku response parsed correctly
good_response = "HEADLINE: AI Risk Hits Critical Mass\n---\nBody text with analysis.\n\n$TSLA $AAPL"
fake_llm = _FakeLlm(good_response)
hc = HaikuDraftComposer(fake_llm)
h, b = hc.compose(_make_narrative(), "new_narrative", "$USO down 2%")
T("TW-H1: good response parsed", h == "AI Risk Hits Critical Mass" and "$TSLA" in b)

# TW-H2: Empty LLM response falls back to template
hc_empty = HaikuDraftComposer(_FakeLlm(""))
h2, b2 = hc_empty.compose(_make_narrative(), "new_narrative", "unavailable")
T("TW-H2: empty response -> template fallback",
  h2 is not None and len(h2) > 0 and b2 is not None and len(b2) > 0)

# TW-H3: None LLM response falls back to template
hc_none = HaikuDraftComposer(_FakeLlm(None))
h3, b3 = hc_none.compose(_make_narrative(), "new_narrative", "unavailable")
T("TW-H3: None response -> template fallback",
  h3 is not None and len(h3) > 0)

# TW-H4: Malformed response (no separator) falls back
hc_bad = HaikuDraftComposer(_FakeLlm("HEADLINE: Test\nNo separator here"))
h4, b4 = hc_bad.compose(_make_narrative(), "new_narrative", "unavailable")
T("TW-H4: missing separator -> template fallback",
  h4 is not None and b4 is not None)

# TW-H5: Headline truncated at 120 chars
long_hl = "HEADLINE: " + "A" * 200 + "\n---\nBody text."
hc_long = HaikuDraftComposer(_FakeLlm(long_hl))
h5, b5 = hc_long.compose(_make_narrative(), "new_narrative", "unavailable")
T("TW-H5: headline truncated to 120", len(h5) <= 120)

# TW-H6: Body truncated at 2000 chars
long_body = "HEADLINE: Test\n---\n" + "B" * 3000
hc_lb = HaikuDraftComposer(_FakeLlm(long_body))
h6, b6 = hc_lb.compose(_make_narrative(), "new_narrative", "unavailable")
T("TW-H6: body truncated to 2000", len(b6) <= 2000)

# TW-H7: Headline trailing period stripped
dotted = "HEADLINE: Something happened.\n---\nBody."
hc_dot = HaikuDraftComposer(_FakeLlm(dotted))
h7, b7 = hc_dot.compose(_make_narrative(), "new_narrative", "unavailable")
T("TW-H7: trailing period stripped", not h7.endswith("."))

# TW-H8: Template fallback includes tickers
hc_fb = HaikuDraftComposer(_FakeLlm("garbage"))
h8, b8 = hc_fb.compose(_make_narrative(), "new_narrative", "$USO down 2%")
T("TW-H8: template fallback has tickers", "$USO" in b8 or "$XLE" in b8)

# TW-H9: compose_draft task type passed to LLM
class _SpyLlm:
    def __init__(self):
        self.last_task_type = None
        self.last_max_tokens = None
    def call_haiku(self, task_type, narrative_id, prompt, max_tokens=None):
        self.last_task_type = task_type
        self.last_max_tokens = max_tokens
        return ""

spy = _SpyLlm()
HaikuDraftComposer(spy).compose(_make_narrative(), "new_narrative", "")
T("TW-H9a: task_type is compose_draft", spy.last_task_type == "compose_draft")
T("TW-H9b: max_tokens is 1024", spy.last_max_tokens == 1024)

# TW-H10: Multiple --- in body preserves content after first separator
multi = "HEADLINE: Test\n---\nPart one.\n---\nPart two."
hc_multi = HaikuDraftComposer(_FakeLlm(multi))
h10, b10 = hc_multi.compose(_make_narrative(), "new_narrative", "")
T("TW-H10: second --- preserved in body", "---" in b10)

# ---------------------------------------------------------------------------
# ChangeDetector: Declining/Dormant exclusion
# ---------------------------------------------------------------------------

S("ChangeDetector: stage exclusion")

det_excl = ChangeDetector(_get_test_repo())

# TW-D11: Declining excluded
n_decl = _make_narrative(stage="Declining", document_count=100)
events_decl = det_excl.find_tweetable_events([n_decl])
T("TW-D11: Declining stage excluded", len(events_decl) == 0)

# TW-D12: Dormant excluded
n_dorm = _make_narrative(stage="Dormant", document_count=100)
events_dorm = det_excl.find_tweetable_events([n_dorm])
T("TW-D12: Dormant stage excluded", len(events_dorm) == 0)

# TW-D13: Emerging still included
n_emrg = _make_narrative(stage="Emerging", document_count=20)
events_emrg = det_excl.find_tweetable_events([n_emrg])
T("TW-D13: Emerging stage included", len(events_emrg) == 1)

# TW-D14: Growing still included
n_grow = _make_narrative(stage="Growing", document_count=20)
events_grow = det_excl.find_tweetable_events([n_grow])
T("TW-D14: Growing stage included", len(events_grow) == 1)

# ---------------------------------------------------------------------------
# _safe_text tests
# ---------------------------------------------------------------------------

S("Helpers: _safe_text and _build_price_context")

T("TW-ST1: _safe_text normal", _safe_text("Hello World") == "Hello World")
T("TW-ST2: _safe_text empty", _safe_text("") == "")
T("TW-ST3: _safe_text None", _safe_text(None) == "")
T("TW-ST4: _safe_text truncates at 200", len(_safe_text("x" * 500)) == 200)
T("TW-ST5: _safe_text strips control chars",
  _safe_text("Hello\x00World\x01") == "HelloWorld")
T("TW-ST6: _safe_text preserves newlines",
  _safe_text("line1\nline2") == "line1\nline2")

# TW-PC1: _build_price_context with no tickers
n_no_tick = _make_narrative(linked_assets="[]")
T("TW-PC1: no tickers -> unavailable",
  _build_price_context(n_no_tick) == "unavailable")

# TW-PC2: _build_price_context with TOPIC-only tickers
n_topic = _make_narrative(linked_assets=json.dumps([{"ticker": "TOPIC:oil prices"}]))
T("TW-PC2: TOPIC-only -> unavailable",
  _build_price_context(n_topic) == "unavailable")

# ---------------------------------------------------------------------------
# Discord constants
# ---------------------------------------------------------------------------

S("Discord constants")

T("TW-DC1: DISCORD_MAX_PER_CYCLE is 10", DISCORD_MAX_PER_CYCLE == 10)

# ---------------------------------------------------------------------------
# Settings tests (Discord)
# ---------------------------------------------------------------------------

S("Settings (Discord)")

from settings import Settings as _Settings
import os
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-settings")
try:
    _s = _Settings()
    T("TW-DS1: DISCORD_WEBHOOK_URL setting exists",
      hasattr(_s, "DISCORD_WEBHOOK_URL"))
    T("TW-DS2: DISCORD_WEBHOOK_ENABLED is bool",
      isinstance(_s.DISCORD_WEBHOOK_ENABLED, bool))
except Exception as e:
    T("TW-DS1: settings load", False, str(e))
    T("TW-DS2: settings load", False, str(e))

# ---------------------------------------------------------------------------
# Settings tests
# ---------------------------------------------------------------------------

S("Settings")

from settings import Settings
import os

# Use a minimal env for testing
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-settings")
try:
    s = Settings()
    T("TW-S1: TWITTER_API_KEY removed from settings", not hasattr(s, "TWITTER_API_KEY"))
    T("TW-S2: TWITTER_ENABLED removed from settings", not hasattr(s, "TWITTER_ENABLED"))
    T("TW-S3: TYPEFULLY_ENABLED removed from settings", not hasattr(s, "TYPEFULLY_ENABLED"))
except Exception as e:
    T("TW-S1: settings load", False, str(e))
    T("TW-S2: settings load", False, str(e))
    T("TW-S3: settings load", False, str(e))

# ---------------------------------------------------------------------------
# Price Validation
# ---------------------------------------------------------------------------

S("Price Validation: _parse_price_claims")

# TW-V1: Standard format extraction
v1 = _parse_price_claims("$TSLA up 4.2% over 5 days ($385.95)")
T("TW-V1: standard format extracts correctly",
  len(v1) == 1 and v1[0]["ticker"] == "TSLA" and v1[0]["direction"] == "up"
  and v1[0]["pct"] == 4.2 and v1[0]["price"] == 385.95)

# TW-V2: Multi-ticker semicolon-separated
v2 = _parse_price_claims(
    "$TSLA up 4.2% over 5 days ($385.95); $NVDA down 1.3% over 5 days ($876.50)")
T("TW-V2: multi-ticker produces 2 claims",
  len(v2) == 2 and v2[0]["ticker"] == "TSLA" and v2[1]["ticker"] == "NVDA")

# TW-V3: Hedged percentage ("roughly")
v3 = _parse_price_claims("$TSLA up roughly 4% this week")
T("TW-V3: hedged pct extracts",
  len(v3) == 1 and v3[0]["pct"] == 4.0 and v3[0]["direction"] == "up")

# TW-V4: Hyphenated ticker (crypto)
v4 = _parse_price_claims("$AVAX-USD down around 4.2% over 5 days ($9.50)")
T("TW-V4: hyphenated ticker extracts",
  len(v4) == 1 and v4[0]["ticker"] == "AVAX-USD"
  and v4[0]["direction"] == "down" and v4[0]["pct"] == 4.2)

# TW-V5: Non-price text returns empty
v5 = _parse_price_claims("The market is quiet today. Nothing to report.")
T("TW-V5: non-price text empty", len(v5) == 0)

# TW-V6: Empty/None returns empty
T("TW-V6a: empty string", len(_parse_price_claims("")) == 0)
T("TW-V6b: None", len(_parse_price_claims(None)) == 0)


S("Price Validation: _validate_price_accuracy")

_src = "$TSLA up 4.2% over 5 days ($385.95)"

# TW-V7: Exact match passes
ok7, r7 = _validate_price_accuracy(
    "$TSLA up 4.2% over 5 days ($385.95)", _src)
T("TW-V7: exact match passes", ok7 and len(r7) == 0)

# TW-V8: Acceptable rounding passes (4.2 -> 4, diff=0.2 < 1.5)
ok8, r8 = _validate_price_accuracy("$TSLA up 4% ($385.95)", _src)
T("TW-V8: acceptable rounding passes", ok8 and len(r8) == 0)

# TW-V9: Gross distortion fails (4.2 -> 1, diff=3.2 > 1.5)
ok9, r9 = _validate_price_accuracy("$TSLA up 1% ($385.95)", _src)
T("TW-V9: gross pct distortion fails", not ok9 and len(r9) > 0)

# TW-V10: Direction flip fails
ok10, r10 = _validate_price_accuracy("$TSLA down 4.2% ($385.95)", _src)
T("TW-V10: direction flip fails", not ok10 and "direction" in r10[0])

# TW-V11: Fabricated ticker fails
ok11, r11 = _validate_price_accuracy(
    "$AAPL up 2% ($150.00)", _src)
T("TW-V11: fabricated ticker fails", not ok11 and "AAPL" in r11[0])

# TW-V12: "unavailable" price_context always passes
ok12, r12 = _validate_price_accuracy(
    "$AAPL up 99% ($1.00)", "unavailable")
T("TW-V12: unavailable context passes", ok12)

# TW-V13: No price claims in generated text passes
ok13, r13 = _validate_price_accuracy(
    "The story is gaining traction.", _src)
T("TW-V13: no claims in output passes", ok13)

# TW-V14: Dollar price divergence fails
ok14, r14 = _validate_price_accuracy(
    "$TSLA up 4.2% ($350.00)", _src)
T("TW-V14: dollar divergence fails",
  not ok14 and "price divergence" in r14[0])

# TW-V14b: Empty price_context passes
ok14b, r14b = _validate_price_accuracy("$TSLA up 5%", "")
T("TW-V14b: empty price_context passes", ok14b)


S("Price Validation: _check_editorial_claims")

# TW-V15: Clean text passes
ok15, f15 = _check_editorial_claims(
    "$TSLA up 4.2% as AI regulation fears spread across the sector.")
T("TW-V15: clean text passes", ok15 and len(f15) == 0)

# TW-V16: "repositioning" flagged
ok16, f16 = _check_editorial_claims(
    "Major players are repositioning ahead of earnings.")
T("TW-V16: repositioning flagged", not ok16 and "repositioning" in f16)

# TW-V17: "smart money" flagged
ok17, f17 = _check_editorial_claims(
    "Smart money is already out of this trade.")
T("TW-V17: smart money flagged", not ok17 and "smart money" in f17)

# TW-V18: "pricing in" flagged
ok18, f18 = _check_editorial_claims(
    "The market is pricing in a rate cut.")
T("TW-V18: pricing in flagged", not ok18 and "pricing in" in f18)

# TW-V19: Multiple flags
ok19, f19 = _check_editorial_claims(
    "Whales are accumulating while insiders sell.")
T("TW-V19: multiple flags",
  not ok19 and len(f19) >= 3)  # whales, accumulating, insiders

# TW-V19b: Empty text passes
ok19b, f19b = _check_editorial_claims("")
T("TW-V19b: empty text passes", ok19b)


S("Price Validation: compose() integration")

# TW-V20: Direction flip in Haiku output -> template fallback
_v20_response = (
    "HEADLINE: TSLA Reverses Hard\n---\n"
    "$TSLA down 4.2% over 5 days ($385.95). The sell-off deepens.\n\n$TSLA"
)
_v20_ctx = "$TSLA up 4.2% over 5 days ($385.95)"
hc_v20 = HaikuDraftComposer(_FakeLlm(_v20_response))
h20, b20 = hc_v20.compose(_make_narrative(), "new_narrative", _v20_ctx)
# Should fall back to template (direction mismatch: Haiku says "down", source says "up")
T("TW-V20: direction flip -> template fallback",
  "independent sources" in b20)

# TW-V21: Editorial claim in Haiku output -> template fallback
_v21_response = (
    "HEADLINE: Quiet Repositioning in Tech\n---\n"
    "Major players are repositioning ahead of $TSLA earnings.\n\n$TSLA"
)
hc_v21 = HaikuDraftComposer(_FakeLlm(_v21_response))
h21, b21 = hc_v21.compose(_make_narrative(), "new_narrative", _v20_ctx)
T("TW-V21: editorial claim -> template fallback",
  "independent sources" in b21)

# TW-V22: Exact price data -> Haiku output accepted
_v22_response = (
    "HEADLINE: AI Risk Hits Insurance\n---\n"
    "$TSLA up 4.2% over 5 days ($385.95) as AI regulation fears spread. "
    "The story has 15 sources and growing.\n\n$TSLA"
)
hc_v22 = HaikuDraftComposer(_FakeLlm(_v22_response))
h22, b22 = hc_v22.compose(_make_narrative(), "new_narrative", _v20_ctx)
T("TW-V22: exact prices -> Haiku accepted",
  h22 == "AI Risk Hits Insurance" and "4.2%" in b22)

# TW-V23: Acceptable rounding -> Haiku output accepted
_v23_response = (
    "HEADLINE: AI Risk Hits Insurance\n---\n"
    "$TSLA up 4% ($385.95) as regulation fears spread.\n\n$TSLA"
)
hc_v23 = HaikuDraftComposer(_FakeLlm(_v23_response))
h23, b23 = hc_v23.compose(_make_narrative(), "new_narrative", _v20_ctx)
T("TW-V23: acceptable rounding -> Haiku accepted",
  h23 == "AI Risk Hits Insurance" and "$TSLA" in b23)

# TW-V24: No price context -> Haiku output accepted regardless
_v24_response = (
    "HEADLINE: Story Gains Traction\n---\n"
    "The theme is accelerating. 15 sources now covering.\n\n$USO"
)
hc_v24 = HaikuDraftComposer(_FakeLlm(_v24_response))
h24, b24 = hc_v24.compose(_make_narrative(), "new_narrative", "unavailable")
T("TW-V24: unavailable context -> Haiku accepted",
  h24 == "Story Gains Traction")

# ---------------------------------------------------------------------------
# Audit: additional edge cases from post-implementation review
# ---------------------------------------------------------------------------

S("Audit: boundary and edge cases")

# TW-A11: Exact boundary tolerance (diff=1.5 passes with > check)
ok_a11, r_a11 = _validate_price_accuracy(
    "$TSLA up 5.7%",
    "$TSLA up 4.2% over 5 days ($385.95)")
T("TW-A11: pct diff exactly 1.5 passes (boundary)", ok_a11)

# TW-A12: Just over boundary (diff=1.6 fails)
ok_a12, r_a12 = _validate_price_accuracy(
    "$TSLA up 5.8%",
    "$TSLA up 4.2% over 5 days ($385.95)")
T("TW-A12: pct diff 1.6 fails (over boundary)", not ok_a12)

# TW-A13: System prompt example headline no longer triggers blacklist
# (Verifies F1 fix: "Pricing In" was replaced in the prompt)
ok_a13, f_a13 = _check_editorial_claims(
    "Insurance Stocks Are Moving on AI Liability Risk")
T("TW-A13: fixed prompt example passes editorial check", ok_a13)

# TW-A14: Original problematic headline IS still caught
ok_a14, f_a14 = _check_editorial_claims(
    "Insurance Stocks Are Quietly Pricing In AI Liability")
T("TW-A14: 'pricing in' still flagged",
  not ok_a14 and "pricing in" in f_a14)

# TW-A15: Duplicate ticker in source keeps first occurrence
ok_a15, r_a15 = _validate_price_accuracy(
    "$TSLA up 4.2% ($385.95)",
    "$TSLA up 4.2% ($385.95); $TSLA down 1.3% ($380.00)")
T("TW-A15: dup ticker in source uses first", ok_a15)

# TW-A16: Cashtag bypass (no $ prefix) passes through unvalidated
ok_a16, r_a16 = _validate_price_accuracy(
    "Tesla shares climbed about 4% last week",
    "$TSLA up 4.2% over 5 days ($385.95)")
T("TW-A16: no-cashtag bypass passes (safe failure mode)", ok_a16)

# TW-A17: MacroTweetComposer rejects editorial claims
_macro_llm = _FakeLlm(
    "Hedge funds are loading up on energy. Smart money sees value.")
mc = MacroTweetComposer(_macro_llm)
macro_result = mc.compose("sector_convergence", {
    "narrative_count": 3, "sector": "Energy",
    "tickers": ["XLE"], "narrative_names": ["A", "B", "C"],
    "ticker_narrative_map": "$XLE: A, B, C",
})
T("TW-A17: macro editorial claim -> template fallback",
  "hedge funds" not in macro_result.lower())

# TW-A18: MacroTweetComposer accepts clean output
_macro_clean = _FakeLlm(
    "Three separate stories are pointing at energy stocks. None reference each other.")
mc2 = MacroTweetComposer(_macro_clean)
clean_result = mc2.compose("sector_convergence", {
    "narrative_count": 3, "sector": "Energy",
    "tickers": ["XLE"], "narrative_names": ["A", "B", "C"],
    "ticker_narrative_map": "$XLE: A, B, C",
})
T("TW-A18: macro clean output accepted",
  "Three separate stories" in clean_result)

# TW-A19: Substring false positive — "insiders" in publication name
# (Documenting known limitation: Business Insiders would trigger)
ok_a19, f_a19 = _check_editorial_claims(
    "Business Insiders reported the earnings beat.")
T("TW-A19: 'insiders' substring match (known limitation)",
  not ok_a19 and "insiders" in f_a19)

# ---------------------------------------------------------------------------
# Macro Dedup Tests
# ---------------------------------------------------------------------------

S("Macro Dedup")

# TW-MD1: _macro_dedup_id generates sector-specific keys for sector patterns
T("TW-MD1: sector_convergence dedup key includes sector",
  _macro_dedup_id("sector_convergence", {"sector": "Technology"})
  == "macro_sector_convergence_technology",
  f"got: {_macro_dedup_id('sector_convergence', {'sector': 'Technology'})}")

# TW-MD2: _macro_dedup_id generates ticker-specific keys
T("TW-MD2: ticker_convergence dedup key includes ticker",
  _macro_dedup_id("ticker_convergence", {"ticker": "NVDA"})
  == "macro_ticker_convergence_NVDA",
  f"got: {_macro_dedup_id('ticker_convergence', {'ticker': 'NVDA'})}")

# TW-MD3: _macro_dedup_id handles missing data gracefully
T("TW-MD3: sector_convergence dedup key with missing sector",
  _macro_dedup_id("sector_convergence", {})
  == "macro_sector_convergence_unknown",
  f"got: {_macro_dedup_id('sector_convergence', {})}")

# TW-MD4: _macro_dedup_id for stage_clustering includes sector
T("TW-MD4: stage_clustering dedup key includes sector",
  _macro_dedup_id("stage_clustering", {"sector": "Health Care"})
  == "macro_stage_clustering_health_care",
  f"got: {_macro_dedup_id('stage_clustering', {'sector': 'Health Care'})}")

# TW-MD5: _macro_dedup_id for volume patterns (global, no key)
T("TW-MD5: high_volume_new dedup key is generic",
  _macro_dedup_id("high_volume_new", {}) == "macro_high_volume_new",
  f"got: {_macro_dedup_id('high_volume_new', {})}")

# TW-MD6: Different sectors produce different dedup keys
_key_tech = _macro_dedup_id("sector_convergence", {"sector": "Technology"})
_key_fin = _macro_dedup_id("sector_convergence", {"sector": "Financials"})
T("TW-MD6: different sectors produce different dedup keys",
  _key_tech != _key_fin,
  f"tech={_key_tech}, fin={_key_fin}")

# TW-MD7: MACRO_COOLDOWN_HOURS constant exists and is reasonable
T("TW-MD7: MACRO_COOLDOWN_HOURS is 48",
  MACRO_COOLDOWN_HOURS == 48,
  f"got: {MACRO_COOLDOWN_HOURS}")

# TW-MD8: Macro dedup integrates with tweet_log via repository
_md_db = os.path.join(_tmp_dir, "tw_md_test.db")
shutil.copy2(_seed_db, _md_db)
_clear_seed_data(_md_db)
_md_repo = SqliteRepository(_md_db)
_md_repo.migrate()
_dedup_key = _macro_dedup_id("sector_convergence", {"sector": "Technology"})
# No previous post — should return None
_last_md = _md_repo.get_last_tweet_for_narrative(_dedup_key)
T("TW-MD8a: no prior macro post returns None",
  _last_md is None,
  f"got: {_last_md}")

# Insert a macro tweet log entry
_md_repo.insert_tweet_log({
    "id": str(uuid.uuid4()),
    "narrative_id": _dedup_key,
    "tweet_id": "discord-test123",
    "tweet_text": "Test macro tweet",
    "tweet_type": "macro_sector_convergence",
    "parent_tweet_id": None,
    "metrics_snapshot": "{}",
    "posted_at": datetime.now(timezone.utc).isoformat(),
    "status": "posted",
})
_last_md2 = _md_repo.get_last_tweet_for_narrative(_dedup_key)
T("TW-MD8b: after insert, get_last_tweet returns the entry",
  _last_md2 is not None and _last_md2["tweet_id"] == "discord-test123",
  f"got: {_last_md2}")

# Different sector key should still return None
_other_key = _macro_dedup_id("sector_convergence", {"sector": "Financials"})
_last_md3 = _md_repo.get_last_tweet_for_narrative(_other_key)
T("TW-MD8c: different sector key returns None (independent cooldowns)",
  _last_md3 is None,
  f"got: {_last_md3}")

# ---------------------------------------------------------------------------
# Raised Threshold Tests
# ---------------------------------------------------------------------------

S("Raised Macro Thresholds")

from unittest.mock import MagicMock, patch

_thr_repo = MagicMock()
_thr_repo.get_top_convergences = MagicMock(return_value=[])
_thr_detector = MacroPatternDetector(_thr_repo)

# TW-TH1: sector_convergence requires 3+ narratives (was 2)
_two_narratives = [
    {"narrative_id": "n1", "stage": "Growing", "linked_assets": '[{"ticker":"CRWD"}]'},
    {"narrative_id": "n2", "stage": "Growing", "linked_assets": '[{"ticker":"ORCL"}]'},
]
with patch.dict("sys.modules", {"api.sector_map": MagicMock(SECTOR_MAP={"CRWD": "Technology", "ORCL": "Technology"})}):
    _sc2 = _thr_detector._sector_convergence(_two_narratives)
T("TW-TH1: 2 narratives in sector no longer fires sector_convergence",
  len(_sc2) == 0,
  f"got: {_sc2}")

_three_narratives = _two_narratives + [
    {"narrative_id": "n3", "stage": "Emerging", "linked_assets": '[{"ticker":"MSFT"}]'},
]
with patch.dict("sys.modules", {"api.sector_map": MagicMock(SECTOR_MAP={"CRWD": "Technology", "ORCL": "Technology", "MSFT": "Technology"})}):
    _sc3 = _thr_detector._sector_convergence(_three_narratives)
T("TW-TH2: 3 narratives in sector fires sector_convergence",
  len(_sc3) == 1,
  f"got: {_sc3}")

# TW-TH3: ticker_convergence requires pressure_score >= 0.5
_thr_repo.get_top_convergences.return_value = [
    {"ticker": "AAPL", "pressure_score": 0.3,
     "contributing_narrative_ids": '["n1","n2"]',
     "direction_agreement": 0.5},
]
_thr_repo.get_narrative = MagicMock(return_value={"name": "Test", "linked_assets": "[]"})
_tc_low = _thr_detector._ticker_convergence()
T("TW-TH3: low pressure_score (0.3) blocked by ticker_convergence",
  len(_tc_low) == 0,
  f"got: {_tc_low}")

_thr_repo.get_top_convergences.return_value = [
    {"ticker": "AAPL", "pressure_score": 0.8,
     "contributing_narrative_ids": '["n1","n2"]',
     "direction_agreement": -0.5},
]
_tc_high = _thr_detector._ticker_convergence()
T("TW-TH4: high pressure_score (0.8) passes ticker_convergence",
  len(_tc_high) == 1,
  f"got: {_tc_high}")

# TW-TH5: sector_direction requires 3+ narratives (was 2)
_two_vel = [
    {"narrative_id": "n1", "stage": "Growing", "velocity": 0.25,
     "linked_assets": '[{"ticker":"CRWD"}]'},
    {"narrative_id": "n2", "stage": "Growing", "velocity": 0.30,
     "linked_assets": '[{"ticker":"ORCL"}]'},
]
with patch.dict("sys.modules", {"api.sector_map": MagicMock(SECTOR_MAP={"CRWD": "Technology", "ORCL": "Technology"})}):
    _sd2 = _thr_detector._sector_direction(_two_vel)
T("TW-TH5: 2 narratives no longer fires sector_direction",
  len(_sd2) == 0,
  f"got: {_sd2}")

# ---------------------------------------------------------------------------
# Template Fallback Quality Tests
# ---------------------------------------------------------------------------

S("Template Fallback Quality")

_tmpl_llm = type("FakeLlm", (), {"call_haiku": lambda *a, **kw: ""})()
_tmpl_mc = MacroTweetComposer(_tmpl_llm)

# TW-TQ1: sector_convergence template no longer says "None reference each other"
_sc_tmpl = _tmpl_mc.compose("sector_convergence", {
    "narrative_count": 3, "sector": "Technology",
    "tickers": ["CRWD", "ORCL"], "narrative_names": ["A", "B", "C"],
    "ticker_descriptions": {"CRWD": "CrowdStrike", "ORCL": "Oracle"},
    "ticker_narrative_map": "$CRWD: A; $ORCL: B, C",
})
T("TW-TQ1: sector_convergence template drops 'None reference each other'",
  "none reference" not in _sc_tmpl.lower(),
  f"got: {_sc_tmpl}")

# TW-TQ2: sector_convergence template mentions the sector
T("TW-TQ2: sector_convergence template mentions sector name",
  "technology" in _sc_tmpl.lower(),
  f"got: {_sc_tmpl}")

# TW-TQ3: sector_convergence template is under 280 chars
T("TW-TQ3: sector_convergence template under 280 chars",
  len(_sc_tmpl) <= 280,
  f"len={len(_sc_tmpl)}")

# TW-TQ4: ticker_convergence template includes direction
_tc_tmpl = _tmpl_mc.compose("ticker_convergence", {
    "company_name": "CrowdStrike", "ticker": "CRWD",
    "narrative_count": 3, "direction": "bearish",
    "narrative_names": ["A", "B", "C"],
})
T("TW-TQ4: ticker_convergence template mentions direction",
  "bearish" in _tc_tmpl.lower(),
  f"got: {_tc_tmpl}")

# TW-TQ5: All templates fit 280 chars
_all_templates_ok = True
_template_tests = [
    ("sector_convergence", {"narrative_count": 5, "sector": "Health Care",
     "tickers": ["UNH", "JNJ", "PFE"], "narrative_names": list("ABCDE"),
     "ticker_descriptions": {"UNH": "UnitedHealth", "JNJ": "J&J", "PFE": "Pfizer"}}),
    ("stage_clustering", {"new_count": 4, "sector": "Technology", "total_emerging": 6}),
    ("sector_accelerating", {"sector": "Financials", "narrative_count": 3}),
    ("sector_decelerating", {"sector": "Energy", "narrative_count": 3}),
    ("high_volume_new", {"emerging_count": 7, "total": 25}),
    ("mature_dominated", {"mature_count": 18, "total": 22}),
    ("ticker_convergence", {"company_name": "American International Group Inc.",
     "ticker": "AIG", "narrative_count": 4, "direction": "bullish",
     "narrative_names": list("ABCD")}),
]
for _pt, _pd in _template_tests:
    _txt = _tmpl_mc.compose(_pt, _pd)
    if len(_txt) > 280:
        _all_templates_ok = False
T("TW-TQ5: all template fallbacks fit 280 chars",
  _all_templates_ok)

# TW-TQ6: No template contains "pointing at" (old phrasing)
_no_pointing = True
for _pt, _pd in _template_tests:
    _txt = _tmpl_mc.compose(_pt, _pd)
    if "pointing at" in _txt.lower():
        _no_pointing = False
T("TW-TQ6: no template uses 'pointing at' phrasing",
  _no_pointing)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print("\n" + "=" * 60)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"  {passed}/{total} passed")
if passed < total:
    print("  FAILED:")
    for name, ok in _results:
        if not ok:
            print(f"    - {name}")
    sys.exit(1)
else:
    print("  All tests passed.")
