"""
Tests for Book-Club-Manager.backend.recommender.event_recommender.

Focus:
- _recency_bonus behavior across time ranges and invalid inputs.
- _normalize_tags for None, string, iterables, and non-iterables.
- _score_event: tag overlap and recency contributions.
- EventRecommender.recommend: ranking, exploration mixing, and duplicate-link suppression.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


def _ensure_inner_project_on_path() -> None:
    """Ensure inner Book-Club-Manager root is on sys.path when running from outer root."""
    # tests/backend/recommender/... → parents[2] is tests/, parent is repo root.
    tests_dir = Path(__file__).resolve().parents[2]
    repo_root = tests_dir.parent
    inner_root = repo_root / "Book-Club-Manager"
    if inner_root.is_dir() and str(inner_root) not in sys.path:
        sys.path.insert(0, str(inner_root))


_ensure_inner_project_on_path()


import backend.recommender.event_recommender as er  # noqa: E402


def test_recency_bonus_handles_none_and_invalid_timestamp() -> None:
    "Test recency bonus handles none and invalid timestamp."
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)

    assert er._recency_bonus(None, now) == 0.0
    # Invalid value should also yield 0.0
    assert er._recency_bonus("not-a-ts", now) == 0.0


def test_recency_bonus_varies_across_time_ranges() -> None:
    "Test recency bonus varies across time ranges."
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    ts_soon = (now + timedelta(days=3)).timestamp()
    ts_mid = (now + timedelta(days=20)).timestamp()
    ts_far = (now + timedelta(days=60)).timestamp()

    soon = er._recency_bonus(ts_soon, now)
    mid = er._recency_bonus(ts_mid, now)
    far = er._recency_bonus(ts_far, now)

    assert soon > mid  # closer events boosted more
    assert mid > far   # far future penalized


def test_normalize_tags_handles_various_inputs() -> None:
    "Test normalize tags handles various inputs."
    assert er._normalize_tags(None) == []
    assert er._normalize_tags("") == []
    assert er._normalize_tags(" Tag ") == ["Tag"]
    assert er._normalize_tags(["A", "  B  ", ""]) == ["A", "B"]
    # Non-iterable other than str yields empty list.
    assert er._normalize_tags(123) == []


def test_score_event_combines_tags_and_recency() -> None:
    "Test score event combines tags and recency."
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    event = {
        "tags": ["Fantasy", "Sci-Fi"],
        "ttl": (now + timedelta(days=5)).timestamp(),
    }
    user_tags = {"Fantasy"}

    score, tag_overlap, tag_score, recency_score = er._score_event(event, user_tags, now)

    assert tag_overlap == 1
    assert tag_score == 1.0
    assert recency_score > 0.0
    assert score > 0.5  # base_score


def test_score_event_handles_non_numeric_time_fields() -> None:
    """Covers the branch where ttl/expiry/start_time cannot be cast to float."""
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    event = {
        "tags": ["Fantasy"],
        "ttl": "not-a-number",  # triggers TypeError/ValueError in float()
    }
    user_tags = {"Fantasy"}

    score, tag_overlap, tag_score, recency_score = er._score_event(event, user_tags, now)

    assert tag_overlap == 1
    assert tag_score == 1.0
    assert recency_score == 0.0
    assert score > 0.5


def test_event_recommender_recommend_ranks_and_mixes_explore_events() -> None:
    "Test event recommender recommend ranks and mixes explore events."
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    base_ts = now.timestamp()
    # Three main events with tag overlap and increasing recency.
    events: List[Dict[str, Any]] = [
        {"event_id": "main1", "tags": ["Fantasy"], "ttl": base_ts + 1},
        {"event_id": "main2", "tags": ["Fantasy"], "ttl": base_ts + 2},
        {"event_id": "main3", "tags": ["Fantasy"], "ttl": base_ts + 3},
        # Explore events with no tag overlap.
        {"event_id": "explore1", "tags": ["Other"], "ttl": base_ts + 4},
        {"event_id": "explore2", "tags": ["Other"], "ttl": base_ts + 5},
    ]
    user_tags = ["Fantasy"]

    # Monkeypatch datetime.now used inside EventRecommender to make deterministic.
    class FixedRecommender(er.EventRecommender):
        def recommend(self, events, user_tags, top_k=10):  # type: ignore[override]
            "Helper for recommend."
            er.datetime = er.datetime  # no-op to keep mypy happy
            return super().recommend(events, user_tags, top_k)

    rec = er.EventRecommender()
    result = rec.recommend(events, user_tags, top_k=5)

    # All results should be from the input set, no duplicates by link/event_id.
    ids = [e["event_id"] for e in result]
    assert len(ids) == len(set(ids))
    assert "main1" in ids and "main2" in ids and "main3" in ids
    assert "explore1" in ids or "explore2" in ids


def test_event_recommender_recommend_exploration_and_backfill_branches() -> None:
    """Exercise duplicate-link skipping, exhausted explore_iter, and ranked backfill."""
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    base_ts = now.timestamp()
    # Two main events with overlap, several explore and ranked-only events sharing links.
    events: List[Dict[str, Any]] = [
        {"event_id": "m1", "tags": ["Fantasy"], "ttl": base_ts + 1, "link": "L1"},
        {"event_id": "m2", "tags": ["Fantasy"], "ttl": base_ts + 2, "link": "L2"},
        # Explore pool: two events, one shares link with later ranked event.
        {"event_id": "e1", "tags": ["Other"], "ttl": base_ts + 3, "link": "E1"},
        {"event_id": "e2", "tags": ["Other"], "ttl": base_ts + 4, "link": "SHARED"},
        # Ranked-only (no overlap, will be in explore_pool) with duplicate link.
        {"event_id": "e3", "tags": ["Other"], "ttl": base_ts + 5, "link": "SHARED"},
    ]
    user_tags = ["Fantasy"]

    rec = er.EventRecommender()
    # Request more than main + unique explore so backfill from ranked is needed.
    result = rec.recommend(events, user_tags, top_k=4)

    links = [str(ev.get("link") or ev.get("event_id")) for ev in result]
    # No duplicate links even though e2/e3 share SHARED.
    assert len(links) == len(set(links))
    # We should have at least one explore event included.
    assert any(ev["tags"] == ["Other"] for ev in result)


def test_event_recommender_recommend_backfills_from_ranked_with_new_link() -> None:
    """Ensure the final ranked backfill loop can add an event that wasn't picked earlier."""
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    base_ts = now.timestamp()
    # One main (overlap) and three explore events; top_k leaves room for backfill.
    events: List[Dict[str, Any]] = [
        {"event_id": "m1", "tags": ["Fantasy"], "ttl": base_ts + 1, "link": "L1"},
        {"event_id": "e1", "tags": ["Other"], "ttl": base_ts + 2, "link": "E1"},
        {"event_id": "e2", "tags": ["Other"], "ttl": base_ts + 3, "link": "E2"},
        {"event_id": "e3", "tags": ["Other"], "ttl": base_ts + 4, "link": "E3"},
    ]
    user_tags = ["Fantasy"]

    rec = er.EventRecommender()
    # With top_k=4, main_pool adds one, stage2 adds two explores,
    # and stage3 backfill should add the remaining ranked event.
    result = rec.recommend(events, user_tags, top_k=4)

    links = {str(ev.get("link") or ev.get("event_id")) for ev in result}
    assert links == {"L1", "E1", "E2", "E3"}


def test_event_recommender_recommend_handles_empty_and_non_positive_top_k() -> None:
    "Test event recommender recommend handles empty and non positive top k."
    rec = er.EventRecommender()

    assert rec.recommend([], ["Fantasy"], top_k=5) == []
    assert rec.recommend([{"event_id": "e1"}], ["Fantasy"], top_k=0) == []

