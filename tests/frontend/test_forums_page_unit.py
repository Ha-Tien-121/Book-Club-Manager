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
        self._text_input_by_key: dict[str, str] = {}
        self._text_input_by_label: dict[str, str] = {}
        self._text_area_by_key: dict[str, str] = {}
        self._radio_value: str = "All"
        self._selectbox_value: str = "Newest first"
        self._form_submit_by_label: dict[str, bool] = {}

        self.rerun_called = 0
        self.warnings: list[str] = []
        self.successes: list[str] = []

    def rerun(self) -> None:
        self.rerun_called += 1

    # layout/output
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

    def warning(self, msg: str, **_kw: Any) -> None:
        self.warnings.append(str(msg))

    def success(self, msg: str, **_kw: Any) -> None:
        self.successes.append(str(msg))

    # containers/forms
    def container(self, **_kw: Any):
        return _FakeCtx()

    def columns(self, n: int | list[int], **_kw: Any):
        if isinstance(n, list):
            n = len(n)
        return [_Column(self) for _ in range(int(n))]

    def expander(self, *_a: Any, **_kw: Any):
        return _FakeCtx()

    def form(self, *_a: Any, **_kw: Any):
        return _FakeCtx()

    # inputs
    def button(self, label: str, *, key: str | None = None, **_kw: Any) -> bool:
        if key and key in self._button_by_key:
            return bool(self._button_by_key[key])
        return bool(self._button_by_label.get(label, False))

    def text_input(self, label: str, *, key: str | None = None, **_kw: Any) -> str:
        if key and key in self._text_input_by_key:
            return str(self._text_input_by_key[key])
        return str(self._text_input_by_label.get(label, ""))

    def text_area(self, _label: str, *, key: str, **_kw: Any) -> str:
        return str(self._text_area_by_key.get(key, ""))

    def form_submit_button(self, label: str, **_kw: Any) -> bool:
        return bool(self._form_submit_by_label.get(label, False))

    def radio(self, _label: str, options: list[str], **_kw: Any) -> str:
        return self._radio_value if self._radio_value in options else options[0]

    def selectbox(self, _label: str, options: list[str], **_kw: Any) -> str:
        return self._selectbox_value if self._selectbox_value in options else options[0]


class _Column:
    """Context-manager column that delegates interactive calls to runtime."""

    def __init__(self, rt: _FakeStreamlitRuntime) -> None:
        self._rt = rt

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def button(self, *a: Any, **kw: Any) -> bool:
        return self._rt.button(*a, **kw)

    def caption(self, *a: Any, **kw: Any) -> None:
        return self._rt.caption(*a, **kw)


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
        "warning",
        "success",
        "container",
        "columns",
        "expander",
        "form",
        "button",
        "text_input",
        "text_area",
        "form_submit_button",
        "radio",
        "selectbox",
    ):
        setattr(st_mod, name, getattr(rt, name))

    sys.modules["streamlit"] = st_mod


def test_forums_detail_signed_in_like_save_and_reply_paths() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)

    forums = importlib.import_module("frontend.pages.forums")
    importlib.reload(forums)

    # Patch pill tags to no-op.
    forums.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]

    saved_forum_db: list[dict] = []
    saved_user_forum: list[dict] = []

    class _Store:
        def save_forum_db(self, db: dict) -> None:
            saved_forum_db.append(db)

        def save_user_forum(self, store: dict) -> None:
            saved_user_forum.append(store)

    forums.get_storage = lambda: _Store()  # type: ignore[assignment]

    rt.session_state["signed_in"] = True
    rt.session_state["user_email"] = "u@example.com"
    rt.session_state["user_name"] = "U"
    rt.session_state["selected_forum_post_id"] = 1

    post = {
        "id": 1,
        "title": "Hello",
        "author": "A",
        "preview": "Body",
        "likes": 0,
        "liked_by": [],
        "replies": 0,
        "comments": [
            {"author": "C", "text": "hi", "likes": 0, "liked_by": []},
        ],
    }
    forum_store = {"posts": [post], "next_post_id": 2}
    store = {"forum": {"u@example.com": {"saved_forum_post_ids": []}}}
    current_user = store["forum"]["u@example.com"]

    # Trigger: like post, save post, like comment, reply submit
    rt._button_by_key["like_post_1"] = True
    rt._button_by_key["save_post_1"] = True
    rt._button_by_key["like_comment_1_0"] = True
    rt._text_area_by_key["reply_text_1"] = "Reply text"
    rt._form_submit_by_label["Reply"] = True

    forums._render_forum_tab(
        tab=_FakeCtx(),
        current_user=current_user,
        store=store,
        forum_store=forum_store,
        forum_posts_data=forum_store["posts"],
        can_view_forum_post=lambda _p, _u: True,
        build_post_tags=forums.build_post_tags,
        format_post_time=forums._format_post_time,
        format_comment_time=forums._format_comment_time,
        forum_preview_text=forums._forum_preview_text,
        clear_aws_bootstrap_cache=lambda: None,
    )

    assert post["likes"] >= 1
    assert "u@example.com" in post.get("liked_by", [])
    assert 1 in current_user.get("saved_forum_post_ids", [])
    assert post["replies"] == len(post.get("comments", []))
    assert saved_forum_db, "expected forum db saves"
    assert saved_user_forum, "expected user forum save"
    assert rt.rerun_called >= 1


def test_forums_detail_reply_empty_warns() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)

    forums = importlib.import_module("frontend.pages.forums")
    importlib.reload(forums)
    forums.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]
    forums.get_storage = lambda: types.SimpleNamespace(save_forum_db=lambda _db: None, save_user_forum=lambda _s: None)  # type: ignore[assignment]

    rt.session_state["signed_in"] = True
    rt.session_state["user_email"] = "u@example.com"
    rt.session_state["user_name"] = "U"
    rt.session_state["selected_forum_post_id"] = 1

    post = {"id": 1, "title": "T", "author": "A", "preview": "P", "comments": []}
    forum_store = {"posts": [post], "next_post_id": 2}
    store = {"forum": {"u@example.com": {"saved_forum_post_ids": []}}}
    current_user = store["forum"]["u@example.com"]

    rt._text_area_by_key["reply_text_1"] = "   "
    rt._form_submit_by_label["Reply"] = True

    forums._render_forum_tab(
        tab=_FakeCtx(),
        current_user=current_user,
        store=store,
        forum_store=forum_store,
        forum_posts_data=forum_store["posts"],
        can_view_forum_post=lambda _p, _u: True,
        build_post_tags=forums.build_post_tags,
        format_post_time=forums._format_post_time,
        format_comment_time=forums._format_comment_time,
        forum_preview_text=forums._forum_preview_text,
        clear_aws_bootstrap_cache=lambda: None,
    )

    assert any("write a reply" in w.lower() for w in rt.warnings)


def test_forums_create_post_success_and_open_discussion_from_list() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)

    forums = importlib.import_module("frontend.pages.forums")
    importlib.reload(forums)
    forums.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]

    saved_forum_db: list[dict] = []

    forums.get_storage = lambda: types.SimpleNamespace(save_forum_db=lambda db: saved_forum_db.append(db), save_user_forum=lambda _s: None)  # type: ignore[assignment]

    rt.session_state["signed_in"] = True
    rt.session_state["user_name"] = "U"

    # Create form submission
    rt._text_input_by_key["forum_new_title"] = "New title"
    rt._text_area_by_key["forum_new_post"] = "New body"
    rt._text_input_by_key["forum_new_tags"] = "mystery, pacing, mystery"
    rt._form_submit_by_label["Post"] = True

    # List view: open the newly created post
    rt._radio_value = "All"
    rt._selectbox_value = "Newest first"
    rt._button_by_key["open_forum_post_1"] = True

    forum_store = {"posts": [], "next_post_id": 1}
    current_user = {"saved_forum_post_ids": []}

    cleared = {"called": 0}

    def _clear_cache() -> None:
        cleared["called"] += 1

    forums._render_forum_create_and_list(
        current_user=current_user,
        forum_store=forum_store,
        forum_posts_data=forum_store["posts"],
        build_post_tags=forums.build_post_tags,
        format_post_time=forums._format_post_time,
        forum_preview_text=lambda s: s,
        clear_aws_bootstrap_cache=_clear_cache,
    )

    assert forum_store["posts"], "expected new post inserted"
    assert forum_store["next_post_id"] == 2
    assert saved_forum_db, "expected save_forum_db called"
    assert cleared["called"] >= 1
    assert rt.session_state.get("forum_form_clear_next") is True
    assert rt.session_state.get("selected_forum_post_id") == 1
    assert rt.rerun_called >= 1

