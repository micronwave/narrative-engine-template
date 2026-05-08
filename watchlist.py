"""
Watchlist Manager — tracks tickers and narratives of interest.
"""

import uuid
from datetime import datetime, timezone

from repository import SqliteRepository


class WatchlistManager:
    def __init__(self, repository: SqliteRepository):
        self.repository = repository

    def create_watchlist(self, user_id: str = "local", name: str = "My Watchlist") -> str:
        """Creates new watchlist. Returns watchlist_id."""
        watchlist_id = str(uuid.uuid4())
        self.repository.create_watchlist({
            "id": watchlist_id,
            "user_id": user_id,
            "name": name,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        return watchlist_id

    def add_item(self, watchlist_id: str, item_type: str, item_id: str) -> str:
        """Adds ticker or narrative to watchlist. Returns item_id."""
        if item_type not in ("ticker", "narrative"):
            raise ValueError("item_type must be 'ticker' or 'narrative'")
        normalized_id = item_id.upper() if item_type == "ticker" else item_id
        existing = self.repository.get_watchlist_items(watchlist_id)
        for item in existing:
            if item["item_type"] == item_type and item["item_id"] == normalized_id:
                raise ValueError(f"{item_type} '{normalized_id}' already in watchlist")
        wl_item_id = str(uuid.uuid4())
        self.repository.add_watchlist_item({
            "id": wl_item_id,
            "watchlist_id": watchlist_id,
            "item_type": item_type,
            "item_id": item_id.upper() if item_type == "ticker" else item_id,
            "added_at": datetime.now(timezone.utc).isoformat(),
        })
        return wl_item_id

    def get_watchlist(self, watchlist_id: str) -> dict | None:
        """Gets watchlist with items."""
        watchlist = self.repository.get_watchlist(watchlist_id)
        if watchlist:
            watchlist["items"] = self.repository.get_watchlist_items(watchlist_id)
        return watchlist

    def list_watchlists(self, user_id: str = "local") -> list[dict]:
        """Lists user's watchlists."""
        return self.repository.list_watchlists(user_id)

