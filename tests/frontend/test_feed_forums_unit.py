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

        self._button_by_label: dict[str, bool] = {}
        self._button_by_key: dict[str, bool] = {}
        self._text_area_by_key: dict[str, str] = {}
        self._form_submit_by_label: dict[str, bool] = {}

        self.rerun_called = 0
        self.warnings: list[str] = []

    def rerun(self) -> None:
        self.rerun_called += 1

    def set_page_config(self, **_kw: Any) -> None:
        return None

    def cache_data(self, **_kw: Any):
        def _decorator(fn):
            fn.clear = lambda: None  # type: ignore[attr-defined]
            return fn

        return _decorator

    # output-ish
    def title(self, *_a: Any, **_kw: Any) -> None:
        return None

    def subheader(self, *_a: Any, **_kw: Any) -> None:
        return None

    def caption(self, *_a: Any, **_kw: Any) -> None:
        return None

    def markdown(self, *_a: Any, **_kw: Any) -> None:
        return None

    def write(self, *_a: Any, **_kw: Any) -> None:
        return None

    def divider(self) -> None:
        return None

    def image(self, *_a: Any, **_kw: Any) -> None:
        return None

    def warning(self, msg: str, **_kw: Any) -> None:
        self.warnings.append(str(msg))

    def info(self, *_a: Any, **_kw: Any) -> None:
        return None

    def success(self, *_a: Any, **_kw: Any) -> None:
        return None

    def selectbox(self, _label: str, *, options: list[str], index: int = 0, key: str | None = None, on_change: Callable[[], None] | None = None, **_kw: Any):  # type: ignore[override]
        # Store selected value into session_state to emulate Streamlit.
        if key:
            try:
                self.session_state[key] = options[index]
            except Exception:
                self.session_state[key] = options[0] if options else None
        if on_change:
            on_change()
        return options[index] if options else None

    # inputs
    def button(self, label: str, *, key: str | None = None, **_kw: Any) -> bool:
        if key and key in self._button_by_key:
            return bool(self._button_by_key[key])
        return bool(self._button_by_label.get(label, False))

    def multiselect(self, *_a: Any, **_kw: Any) -> list[str]:
        return []

    def columns(self, n: int | list[int], **_kw: Any):
        if isinstance(n, list):
            n = len(n)
        return [_FakeCtx() for _ in range(int(n))]

    def form(self, _key: str):
        return _FakeCtx()

    def expander(self, *_a: Any, **_kw: Any):
        return _FakeCtx()

    def text_area(self, _label: str, *, key: str, **_kw: Any) -> str:
        return str(self._text_area_by_key.get(key, ""))

    def form_submit_button(self, label: str, **_kw: Any) -> bool:
        return bool(self._form_submit_by_label.get(label, False))


def _install_streamlit(rt: _FakeStreamlitRuntime) -> None:
    st_mod = types.ModuleType("streamlit")
    st_mod.session_state = rt.session_state  # type: ignore[attr-defined]
    st_mod.query_params = rt.query_params  # type: ignore[attr-defined]
    st_mod.sidebar = rt  # type: ignore[attr-defined]

    for name in (
        "rerun",
        "set_page_config",
        "cache_data",
        "title",
        "subheader",
        "caption",
        "markdown",
        "write",
        "divider",
        "image",
        "warning",
        "info",
        "success",
        "button",
        "multiselect",
        "columns",
        "form",
        "expander",
        "text_area",
        "form_submit_button",
        "selectbox",
    ):
        setattr(st_mod, name, getattr(rt, name))

    # components.html
    comps_pkg = types.ModuleType("streamlit.components")
    comps_v1 = types.ModuleType("streamlit.components.v1")
    comps_v1.html = lambda *_a, **_kw: None  # type: ignore[attr-defined]
    comps_pkg.v1 = comps_v1  # type: ignore[attr-defined]

    sys.modules["streamlit"] = st_mod
    sys.modules["streamlit.components"] = comps_pkg
    sys.modules["streamlit.components.v1"] = comps_v1


def test_forums_helpers_preview_and_tag_filtering() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    forums = importlib.import_module("frontend.pages.forums")
    importlib.reload(forums)

    assert forums._forum_preview_text("") == ""
    assert forums._forum_preview_text("hi", max_chars=10) == "hi"
    assert forums._forum_preview_text("hello world", max_chars=5) == "hello…"

    post = {"tags": [" A ", "A", "B"], "genre": "G", "club": "", "book_title": "T"}
    assert forums.build_post_tags(post) == ["A", "B", "T", "G"]

    posts = [{"tags": ["Fantasy"]}, {"tags": ["Sci-Fi"]}]
    out = forums.filter_posts_by_tag_query(posts, "fan")
    assert out == [posts[0]]


def test_forums_render_detail_missing_resets_selection_and_warns() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    forums = importlib.import_module("frontend.pages.forums")
    importlib.reload(forums)

    rt.session_state["selected_forum_post_id"] = 123

    forums._render_forum_tab(
        tab=_FakeCtx(),
        current_user=None,
        store={},
        forum_store={"posts": []},
        forum_posts_data=[],
        can_view_forum_post=lambda _p, _u: True,
        build_post_tags=forums.build_post_tags,
        format_post_time=forums._format_post_time,
        format_comment_time=forums._format_comment_time,
        forum_preview_text=forums._forum_preview_text,
        clear_aws_bootstrap_cache=lambda: None,
    )

    assert rt.session_state["selected_forum_post_id"] is None
    assert any("not found" in w.lower() for w in rt.warnings)


def test_feed_resolve_recommended_books_fast_path_and_fallback() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    feed = importlib.import_module("frontend.pages.feed")
    importlib.reload(feed)

    ui_book = {
        "id": 1,
        "source_id": "A1",
        "title": "T",
        "author": "A",
        "genres": ["X"],
        "cover": "c",
        "rating": 1,
        "rating_count": 1,
    }
    recs = [ui_book, {"book_id": "A2"}, {"book_id": ""}]
    books_by_source_id = {"A2": {"id": 2, "source_id": "A2", "genres": ["Y"]}}
    fallback = [{"id": 3, "source_id": "A3", "genres": ["X"]}]

    out = feed.resolve_recommended_books(
        recommendations=recs,
        books_by_source_id=books_by_source_id,
        selected_genres=["X"],
        fallback_books=fallback,
        top_k=10,
    )
    # ui_book included + fallback included; A2 filtered by genre
    assert out[0]["source_id"] == "A1"
    assert out[1]["source_id"] == "A3"


def test_feed_render_book_detail_deeplink_builds_minimal_book() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)

    # Patch external lookups inside feed module after import.
    feed = importlib.import_module("frontend.pages.feed")
    importlib.reload(feed)

    rt.session_state["selected_book_source_id"] = "P1"
    rt.session_state["selected_book_id"] = None
    rt.session_state["signed_in"] = False

    def _fake_detail(_sid: str) -> dict:
        return {
            "parent_asin": "P1",
            "title": "Title",
            "author_name": "Auth",
            "images": "img",
            "average_rating": "4.0",
            "rating_number": "2",
            "description": ["a", "b"],
            "categories": ["G1"],
        }

    feed.storage_get_book_details = _fake_detail  # type: ignore[assignment]
    feed.books_service = types.SimpleNamespace(get_book_detail=lambda _sid: {})  # type: ignore[assignment]

    # Render should not crash and should build a minimal book dict path.
    feed.render_book_detail_page(
        books=[],
        books_by_id={},
        extended_books_by_source_id={},
        current_user=None,
        store={},
        forum_store={"posts": []},
        forum_posts_data=[],
        clear_aws_bootstrap_cache=None,
        clear_book_recs_cache=None,
    )

