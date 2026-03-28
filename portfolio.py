"""
Portfolio Manager — tracks holdings and calculates narrative impact.
"""

import csv
import io
import uuid
from datetime import datetime, timezone

from repository import SqliteRepository
from stock_data import StockDataProvider

MAX_IMPORT_ROWS = 1000


class PortfolioManager:
    def __init__(self, repository: SqliteRepository, stock_provider: StockDataProvider):
        self.repository = repository
        self.stock_provider = stock_provider

    def get_or_create_portfolio(self, user_id: str = "local") -> str:
        """Gets existing portfolio or creates new one. Returns portfolio_id."""
        portfolio = self.repository.get_portfolio_by_user(user_id)
        if portfolio:
            return portfolio["id"]
        portfolio_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        self.repository.create_portfolio({
            "id": portfolio_id,
            "user_id": user_id,
            "name": "My Portfolio",
            "created_at": now,
            "updated_at": now,
        })
        return portfolio_id

    def add_holding(self, portfolio_id: str, ticker: str, shares: float, cost_basis: float = None) -> str:
        """Adds holding to portfolio. Returns holding_id."""
        holding_id = str(uuid.uuid4())
        self.repository.add_portfolio_holding({
            "id": holding_id,
            "portfolio_id": portfolio_id,
            "ticker": ticker.upper(),
            "shares": shares,
            "cost_basis": cost_basis,
            "added_at": datetime.now(timezone.utc).isoformat(),
        })
        self.repository.update_portfolio_timestamp(portfolio_id)
        return holding_id

    def remove_holding(self, holding_id: str) -> None:
        """Removes holding from portfolio."""
        holding = self.repository.get_portfolio_holding(holding_id)
        self.repository.delete_portfolio_holding(holding_id)
        if holding:
            self.repository.update_portfolio_timestamp(holding["portfolio_id"])

    def get_holdings(self, portfolio_id: str) -> list[dict]:
        """Gets all holdings enriched with current prices."""
        holdings = self.repository.get_portfolio_holdings(portfolio_id)
        if not holdings:
            return holdings
        tickers = [h["ticker"] for h in holdings]
        quotes = self.stock_provider.get_quotes_batch(tickers)
        for holding in holdings:
            quote = quotes.get(holding["ticker"], {})
            price = float(quote.get("price") or 0)
            holding["current_price"] = price
            holding["current_value"] = round(holding["shares"] * price, 2)
            holding["change_pct"] = float(quote.get("change_pct") or 0)
        return holdings

    def import_csv(self, portfolio_id: str, csv_content: str) -> dict:
        """Imports holdings from CSV. Returns {imported, errors}."""
        reader = csv.DictReader(io.StringIO(csv_content))
        imported = 0
        errors = []
        for row in reader:
            if imported >= MAX_IMPORT_ROWS:
                errors.append(f"Import capped at {MAX_IMPORT_ROWS} rows")
                break
            try:
                ticker = row.get("ticker", "").strip().upper()
                shares = float(row.get("shares", 0))
                cost_basis_raw = row.get("cost_basis", "").strip()
                cost_basis = float(cost_basis_raw) if cost_basis_raw else None
                if not ticker or shares <= 0:
                    errors.append(f"Invalid row: {row}")
                    continue
                self.add_holding(portfolio_id, ticker, shares, cost_basis)
                imported += 1
            except Exception as exc:
                errors.append(f"Error parsing row {row}: {exc}")
        return {"imported": imported, "errors": errors}

    def calculate_impact(self, portfolio_id: str) -> dict:
        """Calculates narrative exposure across portfolio."""
        holdings = self.get_holdings(portfolio_id)
        total_value = sum(h["current_value"] for h in holdings)

        if total_value == 0:
            return {"total_exposure": 0, "narratives_touching": [], "unaffected_holdings": [], "narrative_count": 0}

        narrative_impacts: dict = {}
        affected_tickers: set = set()

        for holding in holdings:
            ticker = holding["ticker"]
            narratives = self.repository.get_narratives_for_ticker(ticker)
            for narrative in narratives:
                nid = narrative["narrative_id"]  # correct PK field
                if nid not in narrative_impacts:
                    narrative_impacts[nid] = {
                        "narrative_id": nid,
                        "narrative_name": narrative["name"],
                        "ns_score": float(narrative.get("ns_score") or 0),
                        "stage": narrative.get("stage") or "Emerging",
                        "affected_holdings": [],
                        "exposure_dollars": 0.0,
                        "exposure_pct": 0.0,
                    }
                narrative_impacts[nid]["affected_holdings"].append(ticker)
                narrative_impacts[nid]["exposure_dollars"] += holding["current_value"]
                affected_tickers.add(ticker)

        for nid, impact in narrative_impacts.items():
            impact["exposure_pct"] = round((impact["exposure_dollars"] / total_value) * 100, 2)
            impact_score = impact["ns_score"] * (impact["exposure_dollars"] / total_value)
            if impact_score > 0.3:
                impact["risk_level"] = "critical"
            elif impact_score > 0.15:
                impact["risk_level"] = "high"
            elif impact_score > 0.05:
                impact["risk_level"] = "medium"
            else:
                impact["risk_level"] = "low"

        unaffected = [h["ticker"] for h in holdings if h["ticker"] not in affected_tickers]

        return {
            "total_exposure": round(total_value, 2),
            "narratives_touching": sorted(narrative_impacts.values(), key=lambda x: x["ns_score"], reverse=True),
            "unaffected_holdings": unaffected,
            "narrative_count": len(narrative_impacts),
        }
