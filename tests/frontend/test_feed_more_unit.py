from __future__ import annotations

import importlib
import sys
import types
from typing import Any, Callable


class _FakeCtx:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeStreamlitRuntime:
    def __init__(self) -> None:
        self.session_state: dict[str, Any] = {}
        self.query_params: dict[str, Any] = {}
        self.sidebar = self

        self._button_by_key: dict[str, bool] = {}
        self._button_by_label: dict[str, bool] = {}
        self._selectbox_choice_by_key: dict[str, str] = {}

        self.captions: list[str] = []
        self.infos: list[str] = []
        self.successes: list[str] = []
        self.warnings: list[str] = []
        self.rerun_called = 0

    def rerun(self) -> None:
        self.rerun_called += 1

    # output/layout
    def title(self, *_a: Any, **_kw: Any) -> None:
        return None

    def subheader(self, *_a: Any, **_kw: Any) -> None:
        return None

    def caption(self, msg: str, **_kw: Any) -> None:
        self.captions.append(str(msg))

    def markdown(self, *_a: Any, **_kw: Any) -> None:
        return None

    def write(self, *_a: Any, **_kw: Any) -> None:
        return None

    def divider(self) -> None:
        return None

    def info(self, msg: str, **_kw: Any) -> None:
        self.infos.append(str(msg))

    def success(self, msg: str, **_kw: Any) -> None:
        self.successes.append(str(msg))

    def warning(self, msg: str, **_kw: Any) -> None:
        self.warnings.append(str(msg))

    def container(self, **_kw: Any):
        return _FakeCtx()

    def columns(self, n: int | list[int], **_kw: Any):
        if isinstance(n, list):
            n = len(n)
        return [_FakeCtx() for _ in range(int(n))]

    def image(self, *_a: Any, **_kw: Any) -> None:
        return None

    # inputs
    def multiselect(self, *_a: Any, **_kw: Any) -> list[str]:
        return []

    def button(self, label: str, *, key: str | None = None, **_kw: Any) -> bool:
        if key and key in self._button_by_key:
            return bool(self._button_by_key[key])
        return bool(self._button_by_label.get(label, False))

    def selectbox(
        self,
        _label: str,
        *,
        options: list[str],
        index: int = 0,
        key: str,
        on_change: Callable[[], None] | None = None,
        **_kw: Any,
    ):
        chosen = self._selectbox_choice_by_key.get(key, options[index] if options else None)
        self.session_state[key] = chosen
        if on_change is not None:
            on_change()
        return chosen

    # forms used later in feed.py; keep no-op to avoid crashes if executed.
    def form(self, *_a: Any, **_kw: Any):
        return _FakeCtx()

    def text_input(self, *_a: Any, **_kw: Any) -> str:
        return ""

    def text_area(self, *_a: Any, **_kw: Any) -> str:
        return ""

    def form_submit_button(self, *_a: Any, **_kw: Any) -> bool:
        return False


def _install_streamlit(rt: _FakeStreamlitRuntime) -> None:
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
        "warning",
        "container",
        "columns",
        "image",
        "multiselect",
        "button",
        "selectbox",
        "form",
        "text_input",
        "text_area",
        "form_submit_button",
    ):
        setattr(st_mod, name, getattr(rt, name))

    sys.modules["streamlit"] = st_mod


def test_feed_tab_handles_recommender_exception_and_empty_states() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)

    feed = importlib.import_module("frontend.pages.feed")
    importlib.reload(feed)

    # Patch heavy UI helpers to no-op.
    feed.render_book_carousel = lambda **_kw: None  # type: ignore[assignment]
    feed.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]
    feed.books_to_ui_shape = lambda raw, _k: list(raw)  # type: ignore[assignment]

    # No trending books, no events.
    cached_spl_trending = lambda: []

    def _bad_cached_book_recommendations(_email: str) -> dict:
        raise KeyError("boom")

    feed._render_feed_tab(
        tab=_FakeCtx(),
        books=[],
        genres=[],
        events=[],
        current_user=None,
        store={},
        books_by_source_id={},
        recommender_available=True,
        cached_spl_trending=cached_spl_trending,
        cached_book_recommendations=_bad_cached_book_recommendations,
        resolve_recommended_books=lambda **_kw: [],
        get_recommended_events_for_user=lambda _email: [],
        format_when=lambda _e: "",
        sync_user_clubs_and_save=lambda _s, _u: None,
        genre_dropdown_options=[],
    )

    assert any("No trending books" in c for c in rt.captions)
    assert any("No recommendations available" in c for c in rt.captions)
    assert any("No suggested events" in c for c in rt.captions)

    # Click "See More Events" branch
    rt._button_by_key["see_more_clubs_feed"] = True
    feed._render_feed_tab(
        tab=_FakeCtx(),
        books=[],
        genres=[],
        events=[],
        current_user=None,
        store={},
        books_by_source_id={},
        recommender_available=False,
        cached_spl_trending=cached_spl_trending,
        cached_book_recommendations=lambda _email: {},
        resolve_recommended_books=lambda **_kw: [],
        get_recommended_events_for_user=lambda _email: [],
        format_when=lambda _e: "",
        sync_user_clubs_and_save=lambda _s, _u: None,
        genre_dropdown_options=[],
    )
    assert rt.session_state.get("jump_to_explore_clubs") is True
    assert rt.rerun_called >= 1


def test_feed_book_detail_library_selectbox_add_and_remove_and_cache_clear() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    feed = importlib.import_module("frontend.pages.feed")
    importlib.reload(feed)

    # Patch pill tags to no-op.
    feed.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]

    # Patch library_service to capture calls.
    calls: dict[str, list[tuple]] = {"add": [], "remove": []}

    feed.library_service = types.SimpleNamespace(
        add_book_to_library=lambda user_id, book_id, shelf, genres_from_book=None: calls["add"].append(
            (user_id, book_id, shelf, genres_from_book)
        ),
        remove_book_from_library=lambda user_id, book_id: calls["remove"].append((user_id, book_id)),
    )

    # Signed-in.
    rt.session_state["signed_in"] = True
    rt.session_state["user_email"] = "u@example.com"

    book = {
        "id": 1,
        "source_id": "P1",
        "title": "Title",
        "author": "A",
        "genres": ["F"],
        "cover": "c",
        "rating": 1,
        "rating_count": 1,
        "description": "desc",
    }
    books = [book]
    books_by_id = {1: book}

    # Pre-seed resolved recs cache keys that should be cleared on add.
    rt.session_state["resolved_recs::u@example.com::x"] = [{"source_id": "P1"}]
    rt.session_state["resolved_recs::u@example.com::y"] = [{"source_id": "P2"}]
    rt.session_state["resolved_recs::other@example.com::z"] = [{"source_id": "P3"}]

    # 1) Add path: choose "Saved"
    current_user = {"user_id": "u@example.com", "library": {"saved": [], "in_progress": [], "finished": []}}
    rt.session_state["selected_book_id"] = 1
    rt._selectbox_choice_by_key["book_lib_status_P1"] = "Saved"

    feed.render_book_detail_page(
        books=books,
        books_by_id=books_by_id,
        extended_books_by_source_id={},
        current_user=current_user,
        store={},
        forum_store={"posts": []},
        forum_posts_data=[],
        clear_aws_bootstrap_cache=None,
        clear_book_recs_cache=lambda: None,
    )

    assert calls["add"], "expected add_book_to_library called"
    assert "resolved_recs::u@example.com::x" not in rt.session_state
    assert "resolved_recs::u@example.com::y" not in rt.session_state
    assert "resolved_recs::other@example.com::z" in rt.session_state
    assert rt.session_state.get("show_book_detail_page") is True

    # 2) Remove path: mark as already saved, choose "Not in library"
    calls["add"].clear()
    calls["remove"].clear()
    current_user2 = {"user_id": "u@example.com", "library": {"saved": ["P1"], "in_progress": [], "finished": []}}
    rt._selectbox_choice_by_key["book_lib_status_P1"] = "Not in library"

    feed.render_book_detail_page(
        books=books,
        books_by_id=books_by_id,
        extended_books_by_source_id={},
        current_user=current_user2,
        store={},
        forum_store={"posts": []},
        forum_posts_data=[],
        clear_aws_bootstrap_cache=None,
        clear_book_recs_cache=None,
    )

    assert calls["remove"], "expected remove_book_from_library called"

