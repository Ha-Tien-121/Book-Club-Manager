"""Event recommender.

This module implements a lightweight scoring-based event recommender that:

- Favors events that overlap with the user's preferred genres / tags.
- Strongly favors near-term events (next ~2 weeks), tapers off by ~45 days, and
  mildly penalizes far-future events.
- Occasionally injects low-overlap events for exploration

It is intentionally simple and stateless: callers provide a pool of candidate
events (e.g. upcoming events from storage.get_soonest_events) plus user tags
and receive an ordered list of event dicts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Dict, Any, Set, Tuple


def _recency_bonus(start_ts: float | int | None, now: datetime) -> float:
    """Recency curve: favor next ~2 weeks, taper, then penalize far future.

    Args:
        start_ts: Event start time as Unix timestamp (seconds), or None.
        now: Current datetime (timezone-aware).
    Returns:
        A float score; higher is better.
    """
    if start_ts is None:
        return 0.0
    try:
        start_dt = datetime.fromtimestamp(float(start_ts), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return 0.0
    days = max((start_dt - now).total_seconds() / 86400.0, 0.0)
    # Boost up to 14 days, taper to 0 by ~45 days, then penalize
    if days <= 14:
        return 3.0 - 0.15 * days  # ~0.9 at 14d
    if days <= 45:
        return 0.9 - 0.03 * (days - 14)  # down to ~0 at 45d
    return -0.05 * (days - 45)  # push farther future down


def _normalize_tags(raw: Any) -> List[str]:
    """Normalize an event's tags field into a list of strings (no lowercasing).

    Tags are kept in the same case/format as provided (aside from stripping),
    so they can be compared directly to user preference tags.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw.strip()] if raw.strip() else []
    if isinstance(raw, Iterable):
        out: List[str] = []
        for t in raw:
            s = str(t).strip()
            if s:
                out.append(s)
        return out
    return []


def _score_event(
    event: Dict[str, Any],
    user_tags: Set[str],
    now: datetime,
) -> Tuple[float, int, float, float]:
    """Compute a composite score for a single event.

    Returns:
        (score, tag_overlap, tag_score, recency_score)
    """
    base_score = 0.5

    event_tags = set(_normalize_tags(event.get("tags")))
    tag_overlap = len(event_tags & user_tags)
    tag_score = float(min(3, tag_overlap))

    # Use ttl / expiry / start_time as the primary time signal
    ts = event.get("ttl") or event.get("expiry") or event.get("start_time")
    try:
        ts_val: float | None = float(ts) if ts is not None else None
    except (TypeError, ValueError):
        ts_val = None
    recency_score = _recency_bonus(ts_val, now)

    # Weighting favors recency first, then tags
    score = (
        base_score
        + 1.5 * recency_score
        + 0.75 * tag_score
    )
    return score, tag_overlap, tag_score, recency_score


class EventRecommender:
    """Scoring-based event recommender operating on in-memory event dicts."""

    def recommend(
        self,
        events: List[Dict[str, Any]],
        user_tags: List[str],
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """Rank events for a user and return up to top_k event dicts.

        Args:
            events: List of event dicts (e.g. from storage.get_soonest_events()).
            user_tags: List of user-preferred tags/genres (strings).
            top_k: Max number of events to return.
        """
        if not events or top_k <= 0:
            return []

        now = datetime.now(tz=timezone.utc)
        # Keep tags in the same format as preferences: no lowercasing, just strip.
        user_tag_set = {str(t).strip() for t in user_tags if str(t).strip()}

        # Compute scores for all events
        scored: List[Dict[str, Any]] = []
        for ev in events:
            score, tag_overlap, tag_score, recency_score = _score_event(
                ev, user_tag_set, now
            )
            item = dict(ev)
            item["_score"] = score
            item["_tag_overlap"] = tag_overlap
            item["_tag_score"] = tag_score
            item["_recency_score"] = recency_score
            scored.append(item)

        # Sort by recency, city, overlap, score, then start_ts ascending
        def sort_key(ev: Dict[str, Any]) -> tuple:
            """Build the primary ranking key for candidate events.

            Args:
                ev: Event payload augmented with intermediate score fields.

            Returns:
                tuple: Sort tuple used for descending priority ordering.

            Exceptions:
                None.
            """
            return (
                ev.get("_recency_score", 0.0),
                ev.get("_tag_overlap", 0),
                ev.get("_score", 0.0),
                ev.get("ttl") or ev.get("expiry") or ev.get("start_time") or 0,
            )

        ranked = sorted(scored, key=sort_key, reverse=True)

        # Exploration: separate overlap vs non-overlap pool
        main_pool = [e for e in ranked if e.get("_tag_overlap", 0) > 0]
        explore_pool = [e for e in ranked if e.get("_tag_overlap", 0) == 0]

        results: List[Dict[str, Any]] = []
        seen_links: set[str] = set()

        def get_link(ev: Dict[str, Any]) -> str:
            """Return a stable deduplication key for an event.

            Args:
                ev: Event payload.

            Returns:
                str: Link if present, otherwise event_id, otherwise object fallback.

            Exceptions:
                None.
            """
            return str(ev.get("link") or ev.get("event_id") or id(ev))

        explore_iter = iter(explore_pool)
        for idx, ev in enumerate(main_pool, start=1):
            if len(results) >= top_k:
                break
            link = get_link(ev)
            if link in seen_links:
                continue
            results.append(ev)
            seen_links.add(link)
            if idx % 3 == 0 and len(results) < top_k:
                try:
                    e = next(explore_iter)
                    link_e = get_link(e)
                    if link_e not in seen_links:
                        results.append(e)
                        seen_links.add(link_e)
                except StopIteration:
                    pass

        # Fill from explore then from ranked if we still have room
        if len(results) < top_k:
            for e in explore_pool:
                if len(results) >= top_k:
                    break
                link_e = get_link(e)
                if link_e not in seen_links:
                    results.append(e)
                    seen_links.add(link_e)
        if len(results) < top_k:
            for r in ranked:
                if len(results) >= top_k:
                    break
                link_r = get_link(r)
                if link_r not in seen_links:
                    results.append(r)
                    seen_links.add(link_r)

        # Final deterministic ordering by cumulative score, then earliest time
        def final_sort_key(ev: Dict[str, Any]) -> tuple:
            """Build final stable sort key after diversification.

            Args:
                ev: Ranked event payload.

            Returns:
                tuple: Composite key of score then time signal.

            Exceptions:
                None.
            """
            return (
                ev.get("_score", 0.0),
                ev.get("ttl") or ev.get("expiry") or ev.get("start_time") or 0,
            )

        results = sorted(results, key=final_sort_key, reverse=True)
        return results[:top_k]

