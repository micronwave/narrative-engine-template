"""
Signal Regression Tests — Ticker Whitelist + Training Contamination Guards

Section 1: extract_known_tickers (6 tests)
  SP7-EKT-1: single-letter false positive rejected by default (min_length=2)
  SP7-EKT-2: single-letter ticker accepted when min_length=1 is explicit
  SP7-EKT-3: NKE extracted from plain mention
  SP7-EKT-4: acronyms (NASA, CEO, WHO) never returned regardless of min_length
  SP7-EKT-5: SO and PR returned (were in old _TICKER_NOISE, now correctly in whitelist)
  SP7-EKT-6: empty string returns []

Section 2: compute_entropy whitelist (4 tests)
  SP7-ENT-1: acronym-only docs produce None (not false entropy)
  SP7-ENT-2: single-letter ticker noise excluded by default
  SP7-ENT-3: explicit single-letter mode allows F/V/C ticker contribution
  SP7-ENT-4: NKE in text raises entropy above None threshold

Section 3: Training dataset contamination guard (4 tests)
  SP7-TRN-1: text_mention asset is skipped by build_training_dataset ticker extraction
  SP7-TRN-2: similarity_score=0.6 (normal) asset is NOT skipped
  SP7-TRN-3: similarity_score<=0.0 asset is skipped even if source missing
  SP7-TRN-4: TOPIC: asset is still skipped regardless of source

Section 6: build_training_dataset observability (4 tests)
  SP7-OBS-1: build_training_dataset emits logger.info summary on mixed fixture
  SP7-OBS-2: log record contains all required field names
  SP7-OBS-3: empty repo returns ([], []) without regression from counter additions
  SP7-OBS-4: non-string ticker values do not crash ticker filtering
"""

import io
import logging
import sys
import json
import tempfile
from unittest.mock import MagicMock, patch
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
_ROOT_PATH = Path(__file__).resolve().parent.parent
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from repository import SqliteRepository
from signal_trainer import build_training_dataset
from signals import compute_entropy, extract_known_tickers, _accept_fallback_ticker

_results = []


def S(section: str):
    print(f"\n--- {section} ---")


def T(name: str, condition: bool, details: str = ""):
    _results.append((name, condition))
    marker = "✓" if condition else "✗"
    msg = f"  [{marker}] {name}"
    if details and not condition:
        msg += f"\n      details: {details}"
    elif details and condition:
        msg += f"  ({details})"
    print(msg)


# ===========================================================================
# Section 1: extract_known_tickers
# ===========================================================================
S("SP7-EKT-1: single-letter false positive rejected by default")
result = extract_known_tickers("A major policy shift could reshape markets.")
T("'A' not in result (min_length=2 default)", "A" not in result, f"got {result}")
T("result is empty for article-only text", result == [], f"got {result}")

S("SP7-EKT-2: single-letter accepted when min_length=1")
result2 = extract_known_tickers("Ford F shares rose on earnings.", min_length=1)
T("'F' found with min_length=1", "F" in result2, f"got {result2}")

S("SP7-EKT-3: NKE extracted from text")
result3 = extract_known_tickers("Nike NKE beat revenue expectations this quarter.")
T("'NKE' in result", "NKE" in result3, f"got {result3}")

S("SP7-EKT-4: acronyms never returned")
result4 = extract_known_tickers("NASA CEO announced WHO and WTO IMF policy.", min_length=1)
for acro in ("NASA", "CEO", "WHO", "WTO", "IMF"):
    T(f"'{acro}' not returned", acro not in result4, f"result={result4}")

S("SP7-EKT-5: SO and PR in whitelist (were in old noise list)")
result5 = extract_known_tickers("Southern Company SO raised dividend. PR Permian Resources.")
T("'SO' found", "SO" in result5, f"got {result5}")
T("'PR' found", "PR" in result5, f"got {result5}")

S("SP7-EKT-6: empty string returns []")
T("empty string -> []", extract_known_tickers("") == [])

# ===========================================================================
# Section 2: compute_entropy whitelist
# ===========================================================================
S("SP7-ENT-1: acronym-only docs produce None")
acro_docs = ["The NASA CEO and WHO announced WTO and IMF policy changes for the UN."]
T("entropy is None for acronym-only text", compute_entropy(acro_docs, 3) is None,
  f"got {compute_entropy(acro_docs, 3)}")

S("SP7-ENT-2: single-letter ticker noise excluded by default")
ford_docs = [
    "Ford (F) shares rose after comments.",
    "Visa (V) moved higher while Citigroup (C) moved lower.",
]
ent_ford = compute_entropy(ford_docs, 3)
T("entropy is None when only single-letter tickers are present by default", ent_ford is None, f"got {ent_ford}")

S("SP7-ENT-3: explicit single-letter mode allows F/V/C contribution")
ent_ford_explicit = compute_entropy(ford_docs, 3, include_single_letter_tickers=True)
T("entropy is not None when include_single_letter_tickers=True", ent_ford_explicit is not None, f"got {ent_ford_explicit}")
T("entropy > 0 in explicit single-letter mode", ent_ford_explicit is not None and ent_ford_explicit > 0,
  f"got {ent_ford_explicit}")

S("SP7-ENT-4: NKE in text raises entropy above threshold")
nke_docs = [
    "Nike NKE raised guidance. Revenue beat expectations by 5%. Gross margin expanded.",
    "NKE stock up 4% on earnings. Consumer demand robust despite macro headwinds.",
    "Nike quarterly earnings show growth across all regions. NKE dividend increased.",
]
ent_nke = compute_entropy(nke_docs, 3)
T("NKE text produces non-None entropy", ent_nke is not None, f"got {ent_nke}")
T("NKE entropy remains non-None in explicit mode",
  compute_entropy(nke_docs, 3, include_single_letter_tickers=True) is not None,
  f"got {ent_nke}")

# ===========================================================================
# Section 3: Training dataset contamination guard
# ===========================================================================
S("SP7-TRN-1: text_mention asset skipped in ticker extraction")

# Simulate the ticker extraction logic from signal_trainer.build_training_dataset
def _extract_training_tickers(linked_assets: list) -> list[str]:
    tickers = []
    for asset in linked_assets:
        if isinstance(asset, dict):
            if asset.get("source") == "text_mention":
                continue
            if float(asset.get("similarity_score", 0.0) or 0.0) <= 0.0:
                continue
            t = asset.get("ticker", "")
        else:
            t = str(asset)
        if t and not t.startswith("TOPIC:"):
            tickers.append(t)
    return tickers

fallback_only = [
    {"ticker": "NKE", "asset_name": "NKE", "similarity_score": 0.0, "source": "text_mention"},
    {"ticker": "A", "asset_name": "A", "similarity_score": 0.0, "source": "text_mention"},
]
tickers = _extract_training_tickers(fallback_only)
T("text_mention assets produce no training tickers", tickers == [],
  f"got {tickers}")

S("SP7-TRN-2: normal similarity asset is not skipped")
normal_asset = [
    {"ticker": "AAPL", "asset_name": "Apple Inc.", "similarity_score": 0.82},
]
tickers2 = _extract_training_tickers(normal_asset)
T("normal similarity_score asset passes through", "AAPL" in tickers2, f"got {tickers2}")

S("SP7-TRN-3: similarity_score<=0.0 skipped even without source")
sim0_asset = [
    {"ticker": "NKE", "asset_name": "Nike Inc.", "similarity_score": 0.0},
]
tickers3 = _extract_training_tickers(sim0_asset)
T("sim0 asset excluded even without source", tickers3 == [], f"got {tickers3}")

S("SP7-TRN-4: TOPIC: asset skipped regardless of source")
topic_asset = [
    {"ticker": "TOPIC:macro_rates", "asset_name": "Macro Rates", "similarity_score": 0.75},
    {"ticker": "MSFT", "asset_name": "Microsoft", "similarity_score": 0.70},
]
tickers4 = _extract_training_tickers(topic_asset)
T("TOPIC: entry excluded", "TOPIC:macro_rates" not in tickers4, f"got {tickers4}")
T("normal ticker still present", "MSFT" in tickers4, f"got {tickers4}")

# ===========================================================================
# Section 4: Fallback acceptance helper
# ===========================================================================
S("SP7-FBP-U1: single plain mention rejected")
u1_excerpts = ["Strong demand helped NKE earnings this quarter."]
T("single plain mention rejected", not _accept_fallback_ticker(u1_excerpts, "NKE"),
  f"got { _accept_fallback_ticker(u1_excerpts, 'NKE') }")

S("SP7-FBP-U2: two plain mentions in one excerpt rejected")
u2_excerpts = ["NKE improved while NKE stayed in focus across the quarter."]
T("two mentions in one excerpt rejected", not _accept_fallback_ticker(u2_excerpts, "NKE"),
  f"got { _accept_fallback_ticker(u2_excerpts, 'NKE') }")

S("SP7-FBP-U3: one plain mention in each of two excerpts accepted")
u3_excerpts = [
    "NKE reported stronger sales and margin expansion.",
    "Separately, NKE guided higher for the next quarter.",
]
T("distinct excerpts accepted", _accept_fallback_ticker(u3_excerpts, "NKE"),
  f"got { _accept_fallback_ticker(u3_excerpts, 'NKE') }")

S("SP7-FBP-U4: single $TICKER mention accepted")
u4_excerpts = ["Watch $NKE after the earnings release."]
T("$mention accepted", _accept_fallback_ticker(u4_excerpts, "NKE"),
  f"got { _accept_fallback_ticker(u4_excerpts, 'NKE') }")

S("SP7-FBP-U5: single (TICKER) mention accepted")
u5_excerpts = ["The market focused on (NKE) after the guidance update."]
T("(TICKER) mention accepted", _accept_fallback_ticker(u5_excerpts, "NKE"),
  f"got { _accept_fallback_ticker(u5_excerpts, 'NKE') }")

S("SP7-FBP-U6: single ( TICKER ) mention accepted")
u6_excerpts = ["The market focused on ( NKE ) after the guidance update."]
T("( TICKER ) mention accepted", _accept_fallback_ticker(u6_excerpts, "NKE"),
  f"got { _accept_fallback_ticker(u6_excerpts, 'NKE') }")

S("SP7-FBP-U7: fallback list empty when all candidates are single plain mentions")
u7_excerpts = [
    "AAPL improved on services growth.",
    "NKE improved on margin expansion.",
]
u7_candidates = extract_known_tickers(" ".join(u7_excerpts))
u7_fallback = [t for t in u7_candidates if _accept_fallback_ticker(u7_excerpts, t)]
T("no ticker accepted from single plain mentions", u7_fallback == [], f"got {u7_fallback}")

S("SP7-FBP-U8: mixed strong forms remain accepted")
u8_excerpts = [
    "NKE improved on margin expansion.",
    "The same note highlighted $AAPL and (MSFT) alongside broader tech strength.",
]
u8_candidates = extract_known_tickers(" ".join(u8_excerpts))
u8_fallback = [t for t in u8_candidates if _accept_fallback_ticker(u8_excerpts, t)]
T("strong fallback forms kept", "AAPL" in u8_fallback and "MSFT" in u8_fallback, f"got {u8_fallback}")
T("plain single mention still rejected", "NKE" not in u8_fallback, f"got {u8_fallback}")

# ===========================================================================
# Section 5: pipeline wiring guard
# ===========================================================================
S("SP7-FBP-I1: pipeline Step 19 calls _accept_fallback_ticker")
pipeline_src = (_ROOT_PATH / "pipeline.py").read_text(encoding="utf-8")
step19_start = pipeline_src.find("# Step 19: Emit Output")
step19_end = pipeline_src.find("# Step 19.1: Catalyst Anchoring (Phase 4)")
step19_block = pipeline_src[step19_start:step19_end] if step19_start != -1 and step19_end != -1 else pipeline_src
T("Step 19 fallback helper call present", "_accept_fallback_ticker(evidence_excerpts, t)" in step19_block,
  "missing helper call in Step 19 block")
T("Step 19 uses distinct evidence excerpts", "evidence_excerpts = [" in step19_block,
  "missing excerpt list in Step 19 block")

S("SP7-FBP-I2: no function-local extract_known_tickers import remains")
T("no local import in Step 19", "from signals import extract_known_tickers" not in step19_block,
  "found stale function-local import")

# ===========================================================================
# Section 6: build_training_dataset observability
# ===========================================================================
S("SP7-OBS-1: mixed fixture exercises filter counters and retained ticker path")
_log_stream_obs = io.StringIO()
_log_handler_obs = logging.StreamHandler(_log_stream_obs)
_log_handler_obs.setLevel(logging.INFO)
_obs_logger = logging.getLogger("signal_trainer")
_obs_logger.addHandler(_log_handler_obs)
_previous_level_obs = _obs_logger.level
_obs_logger.setLevel(logging.INFO)
_obs_repo = MagicMock()
_obs_repo.get_narratives_by_stage.side_effect = lambda stage: [
    {
        "narrative_id": "n-accepted",
        "linked_assets": [
            {"ticker": "NKE", "source": "text_mention", "similarity_score": 0.0},
            {"ticker": "AAPL", "similarity_score": 0.0},
            "TOPIC:macro_rates",
            {"ticker": "MSFT", "similarity_score": 0.75},
        ],
        "entropy": 1.5,
        "velocity_windowed": 0.2,
        "inflow_velocity": 0.1,
        "cross_source_score": 0.3,
        "cohesion": 0.4,
        "intent_weight": 0.5,
        "centrality": 0.6,
        "direction_float": 1.0,
        "confidence": 0.7,
        "certainty_float": 0.8,
        "magnitude_float": 0.9,
        "source_escalation_velocity": 0.1,
        "convergence_exposure": 0.2,
        "catalyst_proximity_score": 0.3,
        "macro_alignment": 0.4,
    },
    {
        "narrative_id": "n-no-tickers",
        "linked_assets": [
            {"ticker": "TSLA", "source": "text_mention", "similarity_score": 0.0},
        ],
        "entropy": 1.5,
        "velocity_windowed": 0.1,
        "inflow_velocity": 0.2,
        "cross_source_score": 0.3,
        "cohesion": 0.4,
        "intent_weight": 0.5,
        "centrality": 0.6,
        "direction_float": 1.0,
        "confidence": 0.7,
        "certainty_float": 0.8,
        "magnitude_float": 0.9,
        "source_escalation_velocity": 0.1,
        "convergence_exposure": 0.2,
        "catalyst_proximity_score": 0.3,
        "macro_alignment": 0.4,
    },
] if stage == "Mature" else []
_obs_repo.get_narrative_signal.return_value = {
    "direction": "bullish",
    "confidence": 0.7,
    "certainty": "confirmed",
    "magnitude": "significant",
}
_obs_repo.get_snapshot_history.return_value = [
    {"velocity": 0.1, "snapshot_date": "2026-01-01"},
]
with patch("stock_data.get_price_history", return_value=[
    {"date": "2026-01-01", "close": 100.0},
    {"date": "2026-01-02", "close": 103.5},
    {"date": "2026-01-08", "close": 106.0},
]):
    _obs_X, _obs_y = build_training_dataset(_obs_repo)
_obs_log_output = _log_stream_obs.getvalue()
_obs_logger.removeHandler(_log_handler_obs)
_obs_logger.setLevel(_previous_level_obs)
T("SP7-OBS-1: logger.info summary emitted",
  "build_training_dataset:" in _obs_log_output,
  f"log output was: {_obs_log_output!r}")
T("SP7-OBS-1: examined counter present",
  "assets_examined=" in _obs_log_output,
  f"log output was: {_obs_log_output!r}")
T("SP7-OBS-1: retained counter present",
  "assets_retained=" in _obs_log_output,
  f"log output was: {_obs_log_output!r}")
T("SP7-OBS-1: accepted sample returned",
  len(_obs_X) == 1 and len(_obs_y) == 1,
  f"X={_obs_X}, y={_obs_y}")

S("SP7-OBS-2: log record contains all required field names")
_REQUIRED_OBS_FIELDS = [
    "evaluated=",
    "assets_examined=",
    "filtered_text_mention=",
    "filtered_zero_score=",
    "filtered_topic=",
    "assets_retained=",
    "no_tickers=",
    "accepted=",
]
for _field in _REQUIRED_OBS_FIELDS:
    T(f"SP7-OBS-2: '{_field}' present in log",
      _field in _obs_log_output,
      f"log output was: {_obs_log_output!r}")
_EXPECTED_OBS_COUNTS = [
    "evaluated=2",
    "assets_examined=5",
    "filtered_text_mention=2",
    "filtered_zero_score=1",
    "filtered_topic=1",
    "assets_retained=1",
    "no_tickers=1",
    "accepted=1",
]
for _count in _EXPECTED_OBS_COUNTS:
    T(f"SP7-OBS-2: '{_count}' present in log",
      _count in _obs_log_output,
      f"log output was: {_obs_log_output!r}")

S("SP7-OBS-3: empty repo returns ([], []) with zero-count summary")
_empty_log_stream_obs = io.StringIO()
_empty_log_handler_obs = logging.StreamHandler(_empty_log_stream_obs)
_empty_log_handler_obs.setLevel(logging.INFO)
_empty_logger_obs = logging.getLogger("signal_trainer")
_empty_logger_obs.addHandler(_empty_log_handler_obs)
_empty_previous_level_obs = _empty_logger_obs.level
_empty_logger_obs.setLevel(logging.INFO)
with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as _empty_tmp_obs:
    _empty_db_path_obs = _empty_tmp_obs.name
try:
    _empty_repo_obs = SqliteRepository(_empty_db_path_obs)
    _empty_repo_obs.migrate()
    _empty_X, _empty_y = build_training_dataset(_empty_repo_obs)
finally:
    Path(_empty_db_path_obs).unlink(missing_ok=True)
_empty_log_output = _empty_log_stream_obs.getvalue()
_empty_logger_obs.removeHandler(_empty_log_handler_obs)
_empty_logger_obs.setLevel(_empty_previous_level_obs)
T("SP7-OBS-3: empty repo X == []", _empty_X == [], f"X={_empty_X}")
T("SP7-OBS-3: empty repo y == []", _empty_y == [], f"y={_empty_y}")
T("SP7-OBS-3: empty repo log emitted",
  "build_training_dataset:" in _empty_log_output,
  f"log output was: {_empty_log_output!r}")
T("SP7-OBS-3: empty repo log zeroed",
  "assets_examined=0" in _empty_log_output and "assets_retained=0" in _empty_log_output,
  f"log output was: {_empty_log_output!r}")
T("SP7-OBS-3: empty repo log has zero narrative count",
  "evaluated=0" in _empty_log_output and "no_tickers=0" in _empty_log_output and "accepted=0" in _empty_log_output,
  f"log output was: {_empty_log_output!r}")

S("SP7-OBS-4: non-string ticker values do not crash ticker filtering")
_malformed_repo_obs = MagicMock()
_malformed_repo_obs.get_narratives_by_stage.side_effect = lambda stage: [
    {
        "narrative_id": "n-malformed-ticker",
        "linked_assets": [
            {"ticker": 123, "similarity_score": 0.8},
        ],
        "entropy": 1.5,
        "velocity_windowed": 0.2,
        "inflow_velocity": 0.1,
        "cross_source_score": 0.3,
        "cohesion": 0.4,
        "intent_weight": 0.5,
        "centrality": 0.6,
        "direction_float": 1.0,
        "confidence": 0.7,
        "certainty_float": 0.8,
        "magnitude_float": 0.9,
        "source_escalation_velocity": 0.1,
        "convergence_exposure": 0.2,
        "catalyst_proximity_score": 0.3,
        "macro_alignment": 0.4,
    },
] if stage == "Mature" else []
_malformed_repo_obs.get_narrative_signal.return_value = {
    "direction": "bullish",
    "confidence": 0.7,
    "certainty": "confirmed",
    "magnitude": "significant",
}
_malformed_repo_obs.get_snapshot_history.return_value = [
    {"velocity": 0.1, "snapshot_date": "2026-01-01"},
]
with patch("stock_data.get_price_history", return_value=[]):
    _malformed_X, _malformed_y = build_training_dataset(_malformed_repo_obs)
T("SP7-OBS-4: malformed ticker safely handled", _malformed_X == [] and _malformed_y == [],
  f"X={_malformed_X}, y={_malformed_y}")

# ===========================================================================
print("\n" + "=" * 50)
passed = sum(1 for _, ok in _results if ok)
total = len(_results)
print(f"SP7 Results: {passed}/{total} passed")
if passed == total:
    print("All SP7 tests passed.")
else:
    failed = [name for name, ok in _results if not ok]
    print(f"FAILED: {failed}")
    sys.exit(1)
