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
        self._selectbox_value: str = "All"
        self._multiselect_value: list[str] = []
        self._tabs_labels: list[str] = []

        self.rerun_called = 0
        self.info_msgs: list[str] = []
        self.success_msgs: list[str] = []
        self.captions: list[str] = []
        self.markdowns: list[str] = []

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

    def write(self, *_a: Any, **_kw: Any) -> None:
        "Helper for write."
        return None

    def divider(self) -> None:
        "Helper for divider."
        return None

    def info(self, msg: str, **_kw: Any) -> None:
        "Helper for info."
        self.info_msgs.append(str(msg))

    def success(self, msg: str, **_kw: Any) -> None:
        "Helper for success."
        self.success_msgs.append(str(msg))

    def markdown(self, text: str, **_kw: Any) -> None:
        "Helper for markdown."
        self.markdowns.append(str(text))

    def link_button(self, *_a: Any, **_kw: Any) -> None:
        "Helper for link button."
        return None

    def container(self, **_kw: Any):
        "Helper for container."
        return _FakeCtx()

    def columns(self, n: int | list[int], **_kw: Any):
        "Helper for columns."
        if isinstance(n, list):
            n = len(n)
        return [_FakeCtx() for _ in range(int(n))]

    def tabs(self, labels: list[str]):
        "Helper for tabs."
        self._tabs_labels = list(labels)
        return [_FakeCtx() for _ in labels]

    # inputs
    def button(self, label: str, *, key: str | None = None, **_kw: Any) -> bool:
        "Helper for button."
        if key and key in self._button_by_key:
            return bool(self._button_by_key[key])
        return bool(self._button_by_label.get(label, False))

    def selectbox(self, _label: str, options: list[str], **_kw: Any) -> str:
        "Helper for selectbox."
        return self._selectbox_value if self._selectbox_value in options else options[0]

    def multiselect(self, *_a: Any, **_kw: Any) -> list[str]:
        "Helper for multiselect."
        return list(self._multiselect_value)


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
        "write",
        "divider",
        "info",
        "success",
        "markdown",
        "link_button",
        "container",
        "columns",
        "tabs",
        "button",
        "selectbox",
        "multiselect",
    ):
        setattr(st_mod, name, getattr(rt, name))

    sys.modules["streamlit"] = st_mod


def test_explore_events_filters_and_save_event_branch() -> None:
    "Test explore events filters and save event branch."
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    explore = importlib.import_module("frontend.pages.explore_events")
    importlib.reload(explore)

    # patch tags renderer
    explore.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]

    events = [
        {
            "event_id": "e1",
            "name": "E1",
            "location": "Seattle, WA",
            "description": "d",
            "tags": ["Fantasy"],
            "external_link": "x",
        },
        {
            "event_id": "e2",
            "name": "E2",
            "location": "Portland, OR",
            "description": "d",
            "tags": ["Romance"],
        },
    ]

    # Filter by city + tag: select Seattle + Fantasy
    rt._selectbox_value = "Seattle"
    rt._multiselect_value = ["Fantasy"]

    rt.session_state["signed_in"] = True
    current_user = {"club_ids": []}
    rt._button_by_key["join_club_e1"] = True

    calls: list[tuple[dict, dict | None]] = []

    def _sync(store: dict, user: dict | None) -> None:
        "Helper for  sync."
        calls.append((store, user))

    explore._render_explore_events_tab(
        tab=_FakeCtx(),
        events=events,
        neighborhoods=["Seattle", "Portland"],
        current_user=current_user,
        store={},
        format_when=lambda _e: "when",
        sync_user_clubs_and_save=_sync,
    )

    assert calls, "expected save-event path"
    assert rt.session_state.get("active_tab_after_save") == "explore_events"
    assert rt.rerun_called == 1

    # Filters that yield empty -> info message
    rt2 = _FakeStreamlitRuntime()
    _install_streamlit(rt2)
    explore = importlib.reload(explore)
    explore.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]
    rt2._selectbox_value = "Seattle"
    rt2._multiselect_value = ["Nonexistent"]
    explore._render_explore_events_tab(
        tab=_FakeCtx(),
        events=events,
        neighborhoods=["Seattle"],
        current_user=None,
        store={},
        format_when=lambda _e: "when",
        sync_user_clubs_and_save=lambda *_a, **_kw: None,
    )
    assert any("No events matching" in m for m in rt2.info_msgs)


def test_my_events_signed_out_and_remove_event_branch() -> None:
    "Test my events signed out and remove event branch."
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    my = importlib.import_module("frontend.pages.my_events")
    importlib.reload(my)
    my.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]

    # Signed out -> info
    rt.session_state["signed_in"] = False
    my._render_my_events_tab(
        tab=_FakeCtx(),
        events=[],
        current_user=None,
        store={},
        format_when=lambda _e: "when",
        sync_user_clubs_and_save=lambda *_a, **_kw: None,
    )
    assert any("Sign in to see your events" in m for m in rt.info_msgs)

    # Signed in, remove event button
    rt2 = _FakeStreamlitRuntime()
    _install_streamlit(rt2)
    my = importlib.reload(my)
    my.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]

    rt2.session_state["signed_in"] = True
    rt2._button_by_key["remove_club_e1"] = True
    current_user = {"club_ids": ["e1"]}
    calls: list[tuple[dict, dict | None]] = []
    my._render_my_events_tab(
        tab=_FakeCtx(),
        events=[{"event_id": "e1", "name": "E1", "location": "Seattle, WA"}],
        current_user=current_user,
        store={},
        format_when=lambda _e: "when",
        sync_user_clubs_and_save=lambda s, u: calls.append((s, u)),
    )
    assert current_user["club_ids"] == []
    assert calls
    assert rt2.rerun_called == 1


def test_library_tab_resolve_fallback_and_signed_out_branch(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    "Test library tab resolve fallback and signed out branch."
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    lib = importlib.import_module("frontend.pages.library")
    importlib.reload(lib)

    # Signed out -> info
    rt.session_state["signed_in"] = False
    lib._render_library_tab(
        tab=_FakeCtx(),
        books_by_id={},
        books_by_source_id={},
        current_user=None,
    )
    assert any("Sign in to see your books" in m for m in rt.info_msgs)

    # Resolve fallback via storage_get_book_details
    monkeypatch.setattr(
        lib,
        "storage_get_book_details",
        lambda _bid: {"parent_asin": "P1", "title": "T", "author_name": "A", "categories": ["Fantasy"]},
    )
    book = lib._resolve_library_book("P1", books_by_id={}, books_by_source_id={})
    assert book is not None
    assert book["source_id"] == "P1"
    assert book["title"] == "T"


def test_render_book_card_view_details_sets_state_and_reruns() -> None:
    "Test render book card view details sets state and reruns."
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    ui = importlib.import_module("frontend.ui.components")
    importlib.reload(ui)

    rt._button_by_key["k_details_P1"] = True
    book = {
        "id": 1,
        "source_id": "P1",
        "title": "T",
        "author": "A",
        "cover": "c",
        "rating": 1,
        "rating_count": 1,
        "genres": ["Fantasy"],
    }
    ui.render_pill_tags = lambda *_a, **_kw: None  # type: ignore[assignment]
    ui.render_book_card(book, key_prefix="k", auth_user="", show_view_details_button=True)
    assert rt.session_state["selected_book_source_id"] == "P1"
    assert rt.session_state["show_book_detail_page"] is True
    assert rt.rerun_called == 1

