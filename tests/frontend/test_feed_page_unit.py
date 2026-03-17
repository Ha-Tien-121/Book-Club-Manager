from __future__ import annotations

import importlib
import sys
import types
from typing import Any


class _FakeCtx:
    def __enter__(self):
        "Support __enter__ for test doubles."
        return self

    def __exit__(self, exc_type, exc, tb):
        "Support __exit__ for test doubles."
        return False


class _FakeStreamlitRuntime:
    def __init__(self) -> None:
        "Support __init__ for test doubles."
        self.session_state: dict[str, Any] = {}
        self.query_params: dict[str, Any] = {}
        self.sidebar = self

        self._button_by_key: dict[str, bool] = {}
        self._button_by_label: dict[str, bool] = {}

        self.rerun_called = 0
        self.captions: list[str] = []
        self.successes: list[str] = []

    def rerun(self) -> None:
        "Helper for rerun."
        self.rerun_called += 1

    # layout/output
    def title(self, *_a: Any, **_kw: Any) -> None:
        "Helper for title."
        return None

    def subheader(self, *_a: Any, **_kw: Any) -> None:
        "Helper for subheader."
        return None

    def caption(self, msg: str, **_kw: Any) -> None:
        "Helper for caption."
        self.captions.append(str(msg))

    def markdown(self, *_a: Any, **_kw: Any) -> None:
        "Helper for markdown."
        return None

    def write(self, *_a: Any, **_kw: Any) -> None:
        "Helper for write."
        return None

    def divider(self) -> None:
        "Helper for divider."
        return None

    def info(self, *_a: Any, **_kw: Any) -> None:
        "Helper for info."
        return None

    def success(self, msg: str, **_kw: Any) -> None:
        "Helper for success."
        self.successes.append(str(msg))

    def container(self, **_kw: Any):
        "Helper for container."
        return _FakeCtx()

    def columns(self, n: int | list[int], **_kw: Any):
        "Helper for columns."
        if isinstance(n, list):
            n = len(n)
        return [_FakeCtx() for _ in range(int(n))]

    def image(self, *_a: Any, **_kw: Any) -> None:
        "Helper for image."
        return None

    def link_button(self, *_a: Any, **_kw: Any) -> None:
        "Helper for link button."
        return None

    def multiselect(self, *_a: Any, **_kw: Any) -> list[str]:
        "Helper for multiselect."
        return []

    def button(self, label: str, *, key: str | None = None, **_kw: Any) -> bool:
        "Helper for button."
        if key and key in self._button_by_key:
            return bool(self._button_by_key[key])
        return bool(self._button_by_label.get(label, False))


def _install_streamlit(rt: _FakeStreamlitRuntime) -> None:
    "Helper for  install streamlit."
    st_mod = types.ModuleType("streamlit")
    st_mod.session_state = rt.session_state  # type: ignore[attr-defined]
    st_mod.query_params = rt.query_params  # type: ignore[attr-defined]
    st_mod.sidebar = rt  # type: ignore[attr-defined]

    for name in (
        "rerun",
        "title",
        "subheader",
        "caption",
        "markdown",
        "write",
        "divider",
        "info",
        "success",
        "container",
        "columns",
        "image",
        "link_button",
        "multiselect",
        "button",
    ):
        setattr(st_mod, name, getattr(rt, name))

    sys.modules["streamlit"] = st_mod


def test_feed_render_tab_hits_trending_recs_and_save_event_paths() -> None:
    "Test feed render tab hits trending recs and save event paths."
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)

    feed = importlib.import_module("frontend.pages.feed")
    importlib.reload(feed)

    # Patch heavy UI helpers to no-op.
    feed.render_book_carousel = lambda **_kw: None  # type: ignore[assignment]
    feed.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]
    feed.books_to_ui_shape = lambda raw, _k: list(raw)  # type: ignore[assignment]

    # Signed-in user so recommended-events and save-event branches execute.
    rt.session_state["signed_in"] = True
    rt.session_state["user_email"] = "u@example.com"

    current_user = {"club_ids": []}
    store: dict[str, Any] = {}

    # Force "Save event" button branch.
    rt._button_by_key["feed_join_club_e1"] = True

    sync_calls: list[tuple[dict, dict | None]] = []

    def _sync_user_clubs_and_save(s: dict, u: dict | None) -> None:
        "Helper for  sync user clubs and save."
        sync_calls.append((s, u))

    cached_spl_trending = lambda: [
        {"id": 1, "genres": ["F"], "rating_count": 10, "title": "T", "author": "A", "cover": "c"}
    ]
    cached_book_recommendations = lambda _email: {
        "book_updated_at": 1,
        "recommended_books": [{"book_id": "P1"}],
    }
    resolve_recommended_books = lambda **_kw: [
        {"id": 9, "genres": ["F"], "rating_count": 1, "title": "R", "author": "A", "cover": "c", "source_id": "P1"}
    ]
    get_recommended_events_for_user = lambda _email: [{"event_id": "e1"}]
    format_when = lambda _e: "soon"

    events = [
        {"event_id": "e1", "name": "E1", "genre": "F", "description": "d" * 10, "location": "Seattle"},
    ]

    books = [
        {"id": 1, "genres": ["F"], "rating_count": 10, "title": "T", "author": "A", "cover": "c", "rating": 4, "rating_count": 10},
    ]

    feed._render_feed_tab(
        tab=_FakeCtx(),
        books=books,
        genres=["F"],
        events=events,
        current_user=current_user,
        store=store,
        books_by_source_id={"P1": books[0]},
        recommender_available=True,
        cached_spl_trending=cached_spl_trending,
        cached_book_recommendations=cached_book_recommendations,
        resolve_recommended_books=resolve_recommended_books,
        get_recommended_events_for_user=get_recommended_events_for_user,
        format_when=format_when,
        sync_user_clubs_and_save=_sync_user_clubs_and_save,
        genre_dropdown_options=["F"],
    )

    assert sync_calls, "expected save-event path to sync user clubs"
    assert rt.rerun_called == 1

    # Call again to hit resolved recommendations session cache branch.
    feed._render_feed_tab(
        tab=_FakeCtx(),
        books=books,
        genres=["F"],
        events=events,
        current_user=current_user,
        store=store,
        books_by_source_id={"P1": books[0]},
        recommender_available=True,
        cached_spl_trending=cached_spl_trending,
        cached_book_recommendations=cached_book_recommendations,
        resolve_recommended_books=lambda **_kw: [],  # would be ignored due to cache hit
        get_recommended_events_for_user=get_recommended_events_for_user,
        format_when=format_when,
        sync_user_clubs_and_save=_sync_user_clubs_and_save,
        genre_dropdown_options=["F"],
    )


def test_feed_book_detail_open_discussion_sets_state_and_reruns() -> None:
    "Test feed book detail open discussion sets state and reruns."
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    feed = importlib.import_module("frontend.pages.feed")
    importlib.reload(feed)

    # Patch UI helpers
    feed.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]

    # Make "Open discussion" button click true.
    rt._button_by_key["open_book_discussion_1"] = True

    rt.session_state["selected_book_source_id"] = None
    rt.session_state["selected_book_id"] = 1
    rt.session_state["signed_in"] = False

    books = [{"id": 1, "source_id": "P1", "title": "Title", "author": "A", "genres": ["F"], "cover": "c", "rating": 1, "rating_count": 1}]
    books_by_id = {1: books[0]}
    forum_posts = [{"id": 1, "title": "D", "author": "U", "tags": ["Title"], "preview": "p", "book_id": 1}]

    feed.render_book_detail_page(
        books=books,
        books_by_id=books_by_id,
        extended_books_by_source_id={},
        current_user=None,
        store={},
        forum_store={"posts": forum_posts},
        forum_posts_data=forum_posts,
        clear_aws_bootstrap_cache=None,
        clear_book_recs_cache=None,
    )

    assert rt.session_state["selected_forum_post_id"] == 1
    assert rt.session_state["jump_to_forum_detail"] is True
    assert rt.session_state["show_book_detail_page"] is False
    assert rt.rerun_called == 1

