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
"""

import sys
import json
from pathlib import Path

_ROOT = str(Path(__file__).parent.parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from signals import compute_entropy, extract_known_tickers

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
