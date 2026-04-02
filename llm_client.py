"""
All LLM calls are logged with full token counts and cost estimates for budget
tracking and audit. Model outputs are used for analysis only and do not
constitute advice.
"""

import logging
import time
import uuid
from datetime import datetime, timezone

import anthropic

from repository import Repository
from settings import Settings

logger = logging.getLogger(__name__)


class BudgetExceededError(Exception):
    """Raised when the daily LLM spend ceiling is reached."""
    pass


# Pricing as of 2026-03. Source: https://docs.anthropic.com/en/docs/about-claude/pricing
# Prices per 1M tokens in USD
HAIKU_INPUT_PRICE_PER_M = 0.80
HAIKU_OUTPUT_PRICE_PER_M = 4.00
SONNET_INPUT_PRICE_PER_M = 3.00
SONNET_OUTPUT_PRICE_PER_M = 15.00
# NOTE: These are for claude-haiku-4-5-20251001 and claude-sonnet-4-6
# Review https://www.anthropic.com/pricing when updating model versions

_HAIKU_FALLBACKS: dict[str, str] = {
    "label_narrative": "Unlabeled Narrative",
    "classify_topic": "",
    "classify_stage": "Emerging",
    "summarize_mutation_fallback": "Analysis unavailable",
    "validate_cluster": "SCORE: 1.0 | REASON: Validation unavailable, accepting cluster.",
    "mutation_explanation": "Mutation explanation unavailable.",
    "deep_analysis": '{"thesis":"Analysis unavailable","key_drivers":[],"asset_impact":[],"risk_factors":[],"historical_comparison":null}',
    "extract_signal": '{"direction":"neutral","confidence":0.0,"timeframe":"unknown","magnitude":"incremental","certainty":"speculative","key_actors":[],"affected_sectors":[],"catalyst_type":"unknown"}',
    "compose_draft": "",
}


def parse_signal_json(text: str, fallback: str | None = None) -> dict:
    """
    Extract a JSON signal object from LLM response text.

    Three-tier strategy:
      1. Find line starting with SIGNAL_JSON: and parse what follows
      2. Find any {…} block containing a "direction" key
      3. Return parsed fallback default

    Never raises. Returns a raw dict (caller should pass to validate_signal_fields).
    """
    import json

    # Tier 1: line-prefix extraction
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.upper().startswith("SIGNAL_JSON:"):
            json_str = stripped[len("SIGNAL_JSON:"):].strip()
            try:
                parsed = json.loads(json_str)
                if isinstance(parsed, dict):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass
            break  # found the prefix line but couldn't parse — fall through

    # Tier 2: brace-matching extraction — find {…} blocks containing "direction"
    try:
        i = 0
        while i < len(text):
            start = text.find("{", i)
            if start == -1:
                break
            depth = 0
            for j in range(start, len(text)):
                if text[j] == "{":
                    depth += 1
                elif text[j] == "}":
                    depth -= 1
                    if depth == 0:
                        candidate = text[start : j + 1]
                        try:
                            parsed = json.loads(candidate)
                            if isinstance(parsed, dict) and "direction" in parsed:
                                logger.debug(
                                    "parse_signal_json: extracted via brace-matching fallback"
                                )
                                return parsed
                        except (json.JSONDecodeError, TypeError):
                            pass
                        break
            i = start + 1
    except Exception:
        pass

    # Tier 3: final fallback
    logger.debug("parse_signal_json: using hardcoded fallback")
    try:
        return json.loads(fallback or _HAIKU_FALLBACKS["extract_signal"])
    except (json.JSONDecodeError, TypeError):
        return {"direction": "neutral", "confidence": 0.0, "timeframe": "unknown",
                "magnitude": "incremental", "certainty": "speculative",
                "key_actors": [], "affected_sectors": [], "catalyst_type": "unknown"}


class LlmClient:
    """
    Single interface for all LLM calls.
    No direct anthropic SDK calls outside this file.

    # TODO SCALE: replace SQLite counter with Redis INCR for atomic budget tracking under concurrent workers
    """

    def __init__(self, settings: Settings, repository: Repository) -> None:
        self._settings = settings
        self._repository = repository
        self._client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

    # ------------------------------------------------------------------
    # Token estimation
    # ------------------------------------------------------------------

    def estimate_tokens(self, text: str) -> int:
        """Estimate tokens as int(len(text.split()) * 1.3)."""
        return int(len(text.split()) * 1.3)

    # ------------------------------------------------------------------
    # Sonnet gate checks
    # ------------------------------------------------------------------

    def check_sonnet_gates(
        self,
        narrative_id: str,
        narrative_created_at: str,
        estimated_tokens: int,
    ) -> tuple[bool, str]:
        """
        Check all 4 Sonnet gates in order 1→4.
        Returns (all_passed, reason_if_failed).
        """
        # Gate 1: ns_score > CONFIDENCE_ESCALATION_THRESHOLD
        narrative = self._repository.get_narrative(narrative_id)
        if narrative is None:
            return False, "gate_1_narrative_not_found"
        ns_score = float(narrative.get("ns_score") or 0.0)
        if ns_score <= self._settings.CONFIDENCE_ESCALATION_THRESHOLD:
            logger.debug(
                "Sonnet gate 1 failed for %s: ns_score=%.4f <= threshold=%.4f",
                narrative_id,
                ns_score,
                self._settings.CONFIDENCE_ESCALATION_THRESHOLD,
            )
            return False, f"gate_1_ns_score: {ns_score:.4f}"

        # Gate 2: narrative age >= 2 consecutive daily cycles
        from signals import get_narrative_age_days
        age_days = get_narrative_age_days(narrative_created_at)
        # get_narrative_age_days returns 0 on parse failure, which is
        # correctly caught by the < 2 check (fails safe).
        if age_days < 2:
            logger.debug(
                "Sonnet gate 2 failed for %s: age=%d day(s) < 2",
                narrative_id,
                age_days,
            )
            return False, f"gate_2_age: {age_days} day(s)"

        # Gate 3: no Sonnet call for this narrative in the last 24 hours
        recent_calls = self._repository.get_sonnet_calls_last_24h(narrative_id)
        if recent_calls:
            logger.debug(
                "Sonnet gate 3 failed for %s: %d recent call(s) in last 24h",
                narrative_id,
                len(recent_calls),
            )
            return False, f"gate_3_recent_sonnet_call: {len(recent_calls)} call(s)"

        # Gate 4: estimated_tokens + today's spend < SONNET_DAILY_TOKEN_BUDGET
        today_str = datetime.now(timezone.utc).date().isoformat()
        spend_record = self._repository.get_sonnet_daily_spend(today_str)
        tokens_used = int((spend_record.get("total_tokens_used") or 0) if spend_record else 0)
        if tokens_used + estimated_tokens >= self._settings.SONNET_DAILY_TOKEN_BUDGET:
            return (
                False,
                f"gate_4_budget_ceiling: used={tokens_used}, est={estimated_tokens}, "
                f"budget={self._settings.SONNET_DAILY_TOKEN_BUDGET}",
            )

        return True, ""

    # ------------------------------------------------------------------
    # Haiku calls
    # ------------------------------------------------------------------

    def call_haiku(self, task_type: str, narrative_id: str, prompt: str,
                   max_tokens: int | None = None) -> str:
        """
        Execute Haiku call with retry logic and logging.
        Falls back to task-specific defaults on failure.

        task_type: 'label_narrative' | 'classify_stage' | 'summarize_mutation_fallback' | 'compose_draft'
        max_tokens: override for HAIKU_MAX_TOKENS (default: settings value)
        """
        daily_spend = self._repository.get_daily_llm_spend()
        if daily_spend >= self._settings.LLM_DAILY_BUDGET_USD:
            raise BudgetExceededError(
                f"Daily LLM budget exceeded (${daily_spend:.2f} / ${self._settings.LLM_DAILY_BUDGET_USD:.2f})"
            )

        effective_max_tokens = max_tokens if max_tokens is not None else self._settings.HAIKU_MAX_TOKENS
        backoff_delays = [1, 3]
        last_exc: Exception | None = None

        for attempt in range(3):  # initial attempt + 2 retries
            try:
                response = self._client.messages.create(
                    model=self._settings.HAIKU_MODEL,
                    max_tokens=effective_max_tokens,
                    messages=[{"role": "user", "content": prompt}],
                )
                result_text: str = response.content[0].text if response.content else ""

                input_tokens: int = response.usage.input_tokens
                output_tokens: int = response.usage.output_tokens
                cost = (
                    (input_tokens * HAIKU_INPUT_PRICE_PER_M / 1_000_000)
                    + (output_tokens * HAIKU_OUTPUT_PRICE_PER_M / 1_000_000)
                )
                self._repository.log_llm_call(
                    {
                        "call_id": str(uuid.uuid4()),
                        "narrative_id": narrative_id,
                        "model": self._settings.HAIKU_MODEL,
                        "task_type": task_type,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "cost_estimate_usd": cost,
                        "called_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                return result_text

            except Exception as exc:
                last_exc = exc
                if attempt < 2:
                    delay = backoff_delays[attempt]
                    logger.warning(
                        "Haiku call attempt %d failed for %s (task=%s): %s — retrying in %ds",
                        attempt + 1,
                        narrative_id,
                        task_type,
                        exc,
                        delay,
                    )
                    time.sleep(delay)

        # All retries exhausted
        logger.error(
            "Haiku call failed for %s (task=%s) after 3 attempts: %s",
            narrative_id,
            task_type,
            last_exc,
        )
        self._log_pipeline_error(
            step_name=f"haiku_call_failed:{task_type}",
            error_message=str(last_exc),
        )
        return _HAIKU_FALLBACKS.get(task_type, "Analysis unavailable")

    # ------------------------------------------------------------------
    # Sonnet calls
    # ------------------------------------------------------------------

    def call_sonnet(self, narrative_id: str, prompt: str) -> str | None:
        """
        Check all 4 gates, execute Sonnet if passed.
        Returns None if gates 1–3 fail (narrative not yet eligible).
        Falls back to Haiku summarize_mutation_fallback if gate 4 (budget) fails.
        Falls back to Haiku summarize_mutation_fallback if API call fails.
        """
        narrative = self._repository.get_narrative(narrative_id)
        if narrative is None:
            logger.warning("call_sonnet: narrative %s not found in repository", narrative_id)
            return None

        narrative_created_at: str = narrative.get("created_at") or ""
        # Conservative token estimate: input prompt + max output tokens
        estimated_tokens = self.estimate_tokens(prompt) + self._settings.SONNET_MAX_TOKENS

        passed, reason = self.check_sonnet_gates(
            narrative_id, narrative_created_at, estimated_tokens
        )

        if not passed:
            if reason.startswith("gate_4"):
                # Budget ceiling hit — do NOT silently skip
                today_str = datetime.now(timezone.utc).date().isoformat()
                spend_record = self._repository.get_sonnet_daily_spend(today_str)
                current_spend = int(
                    (spend_record.get("total_tokens_used") or 0) if spend_record else 0
                )
                logger.warning(
                    "BUDGET_CEILING_HIT: narrative_id=%s estimated_tokens=%d "
                    "current_spend=%d budget=%d",
                    narrative_id,
                    estimated_tokens,
                    current_spend,
                    self._settings.SONNET_DAILY_TOKEN_BUDGET,
                )
                self._log_pipeline_error(
                    step_name="BUDGET_CEILING_HIT",
                    error_message=(
                        f"narrative_id={narrative_id} estimated_tokens={estimated_tokens} "
                        f"current_spend={current_spend} "
                        f"budget={self._settings.SONNET_DAILY_TOKEN_BUDGET}"
                    ),
                    status="WARNING",
                )
                return self.call_haiku("summarize_mutation_fallback", narrative_id, prompt)

            # Gates 1–3 failed — narrative not eligible, return None
            logger.debug("Sonnet gates failed for %s: %s", narrative_id, reason)
            return None

        # All gates passed — attempt Sonnet call
        try:
            response = self._client.messages.create(
                model=self._settings.SONNET_MODEL,
                max_tokens=self._settings.SONNET_MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
            result_text = response.content[0].text if response.content else ""

            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            cost = (
                (input_tokens * SONNET_INPUT_PRICE_PER_M / 1_000_000)
                + (output_tokens * SONNET_OUTPUT_PRICE_PER_M / 1_000_000)
            )
            self._repository.log_llm_call(
                {
                    "call_id": str(uuid.uuid4()),
                    "narrative_id": narrative_id,
                    "model": self._settings.SONNET_MODEL,
                    "task_type": "mutation_analysis",
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost_estimate_usd": cost,
                    "called_at": datetime.now(timezone.utc).isoformat(),
                }
            )

            today_str = datetime.now(timezone.utc).date().isoformat()
            # TODO SCALE: replace SQLite counter with Redis INCR for atomic budget tracking under concurrent workers
            self._repository.update_sonnet_daily_spend(
                today_str, input_tokens + output_tokens, 1
            )

            return result_text

        except Exception as exc:
            logger.error(
                "Sonnet call failed for %s after passing all gates: %s — falling back to Haiku",
                narrative_id,
                exc,
            )
            self._log_pipeline_error(
                step_name="sonnet_call_failed",
                error_message=str(exc),
            )
            return self.call_haiku("summarize_mutation_fallback", narrative_id, prompt)

    # ------------------------------------------------------------------
    # Haiku chat (multi-turn)
    # ------------------------------------------------------------------

    def call_haiku_chat(self, system_prompt: str, messages: list[dict]) -> dict:
        """Multi-turn chat with Haiku. Returns {content, tokens, cost}."""
        daily_spend = self._repository.get_daily_llm_spend()
        if daily_spend >= self._settings.LLM_DAILY_BUDGET_USD:
            raise BudgetExceededError(
                f"Daily LLM budget exceeded (${daily_spend:.2f} / ${self._settings.LLM_DAILY_BUDGET_USD:.2f})"
            )

        try:
            response = self._client.messages.create(
                model=self._settings.HAIKU_MODEL,
                max_tokens=500,
                system=system_prompt,
                messages=messages,
            )
            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            cost = self._calculate_haiku_cost(input_tokens, output_tokens)

            self._repository.log_llm_call({
                "call_id": str(uuid.uuid4()),
                "narrative_id": None,
                "model": self._settings.HAIKU_MODEL,
                "task_type": "chat",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_estimate_usd": cost,
                "called_at": datetime.now(timezone.utc).isoformat(),
            })
            return {
                "content": response.content[0].text if response.content else "",
                "tokens": input_tokens + output_tokens,
                "cost": cost,
            }
        except Exception as exc:
            logger.warning("call_haiku_chat failed: %s", exc)
            return {"content": "I'm unable to process that request right now.", "tokens": 0, "cost": 0.0}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _calculate_haiku_cost(self, input_tokens: int, output_tokens: int) -> float:
        return (
            (input_tokens * HAIKU_INPUT_PRICE_PER_M / 1_000_000)
            + (output_tokens * HAIKU_OUTPUT_PRICE_PER_M / 1_000_000)
        )

    def _log_pipeline_error(
        self, step_name: str, error_message: str, status: str = "ERROR"
    ) -> None:
        """Best-effort pipeline run log entry. Never raises."""
        try:
            self._repository.log_pipeline_run(
                {
                    "run_id": str(uuid.uuid4()),
                    "step_number": None,
                    "step_name": step_name,
                    "status": status,
                    "error_message": error_message,
                    "duration_ms": None,
                    "run_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        except Exception as exc:
            logger.warning("Could not write pipeline_run_log for %s: %s", step_name, exc)
