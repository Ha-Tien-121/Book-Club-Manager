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


class _QueryParams(dict):
    def clear(self) -> None:  # match streamlit query_params API used by app
        super().clear()


class _FakeSidebar:
    def __init__(self, rt: "_FakeStreamlitRuntime") -> None:
        self._rt = rt
        self.errors: list[str] = []
        self.successes: list[str] = []

    def title(self, *_a: Any, **_kw: Any) -> None:
        return None

    def subheader(self, *_a: Any, **_kw: Any) -> None:
        return None

    def markdown(self, *_a: Any, **_kw: Any) -> None:
        return None

    def caption(self, *_a: Any, **_kw: Any) -> None:
        return None

    def error(self, msg: str, **_kw: Any) -> None:
        self.errors.append(str(msg))

    def success(self, msg: str, **_kw: Any) -> None:
        self.successes.append(str(msg))

    def button(self, label: str, **_kw: Any) -> bool:
        return self._rt.button(label, **_kw)

    def form(self, *_a: Any, **_kw: Any):
        return _FakeCtx()


class _FakeStreamlitRuntime:
    def __init__(self) -> None:
        self.session_state: dict[str, Any] = {}
        self.query_params: _QueryParams = _QueryParams()
        self.sidebar = _FakeSidebar(self)

        self._button_by_label: dict[str, bool] = {}
        self._button_by_key: dict[str, bool] = {}
        self._text_input_by_key: dict[str, str] = {}

        self.rerun_called = 0

    def rerun(self) -> None:
        self.rerun_called += 1

    def set_page_config(self, **_kw: Any) -> None:
        return None

    def cache_data(self, **_kw: Any):
        def _decorator(fn):
            fn.clear = lambda: None  # type: ignore[attr-defined]
            return fn

        return _decorator

    # layout/output
    def title(self, *_a: Any, **_kw: Any) -> None:
        return None

    def caption(self, *_a: Any, **_kw: Any) -> None:
        return None

    def markdown(self, *_a: Any, **_kw: Any) -> None:
        return None

    def write(self, *_a: Any, **_kw: Any) -> None:
        return None

    def success(self, *_a: Any, **_kw: Any) -> None:
        return None

    def error(self, *_a: Any, **_kw: Any) -> None:
        return None

    def checkbox(self, *_a: Any, **_kw: Any) -> bool:
        return False

    def columns(self, n: int, **_kw: Any):
        return [_FakeCtx() for _ in range(int(n))]

    def tabs(self, labels: list[str]):
        return [_FakeCtx() for _ in labels]

    # inputs
    def button(self, label: str, *, key: str | None = None, **_kw: Any) -> bool:
        if key and key in self._button_by_key:
            return bool(self._button_by_key[key])
        return bool(self._button_by_label.get(label, False))

    def text_input(self, _label: str, *, key: str | None = None, **_kw: Any) -> str:
        if key:
            return str(self._text_input_by_key.get(key, ""))
        return ""

    def form(self, *_a: Any, **_kw: Any):
        return _FakeCtx()

    def form_submit_button(self, label: str, **_kw: Any) -> bool:
        return bool(self._button_by_label.get(label, False))


def _install_streamlit(rt: _FakeStreamlitRuntime) -> None:
    st_mod = types.ModuleType("streamlit")
    st_mod.session_state = rt.session_state  # type: ignore[attr-defined]
    st_mod.query_params = rt.query_params  # type: ignore[attr-defined]
    st_mod.sidebar = rt.sidebar  # type: ignore[attr-defined]

    for name in (
        "rerun",
        "set_page_config",
        "cache_data",
        "title",
        "caption",
        "markdown",
        "write",
        "success",
        "error",
        "checkbox",
        "columns",
        "tabs",
        "button",
        "text_input",
        "form",
        "form_submit_button",
    ):
        setattr(st_mod, name, getattr(rt, name))

    # `streamlit.components.v1` import in main.py
    comps_pkg = types.ModuleType("streamlit.components")
    comps_v1 = types.ModuleType("streamlit.components.v1")
    comps_v1.html = lambda *_a, **_kw: None  # type: ignore[attr-defined]
    comps_pkg.v1 = comps_v1  # type: ignore[attr-defined]

    sys.modules["streamlit"] = st_mod
    sys.modules["streamlit.components"] = comps_pkg
    sys.modules["streamlit.components.v1"] = comps_v1


def test_auth_panel_sign_out_clears_session_and_reruns() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    auth = importlib.import_module("frontend.pages.auth")
    importlib.reload(auth)

    rt.session_state["signed_in"] = True
    rt.session_state["user_email"] = "u@example.com"
    rt.session_state["user_name"] = "U"
    rt._button_by_label["Sign out"] = True

    auth.auth_panel()

    assert rt.session_state["signed_in"] is False
    assert rt.session_state["user_email"] == ""
    assert rt.session_state["user_name"] == ""
    assert rt.rerun_called == 1


def test_auth_panel_sign_in_validation_and_success_paths(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    auth = importlib.import_module("frontend.pages.auth")
    importlib.reload(auth)

    # Validation: missing password
    rt.session_state["signed_in"] = False
    rt._text_input_by_key["auth_signin_email"] = "u@example.com"
    rt._text_input_by_key["auth_signin_password"] = ""
    rt._button_by_label["Sign in"] = True

    auth.auth_panel()
    assert rt.sidebar.errors

    # Success path
    rt.sidebar.errors.clear()
    rt._text_input_by_key["auth_signin_password"] = "pw"

    monkeypatch.setattr(auth, "login_user", lambda **_kw: {"name": "User"})  # type: ignore[arg-type]
    auth.auth_panel()

    assert rt.session_state["signed_in"] is True
    assert rt.session_state["user_email"] == "u@example.com"
    assert rt.session_state["user_name"] == "User"
    assert rt.rerun_called >= 1


def test_main_create_account_early_return() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    main = importlib.import_module("frontend.main")
    importlib.reload(main)

    # Patch heavy dependencies to no-op
    main.inject_styles = lambda: None  # type: ignore[assignment]
    main.books_service = types.SimpleNamespace(get_trending_books_reviews=lambda _n: [], get_trending_books_spl=lambda _n: [])  # type: ignore[assignment]
    main.events_service = types.SimpleNamespace(get_explore_events=lambda _n: [])  # type: ignore[assignment]
    main.build_ui_bootstrap = lambda _books, _events, _posts: {  # type: ignore[assignment]
        "books": [],
        "books_by_id": {},
        "books_by_source_id": {},
        "clubs": [],
        "genres": [],
        "neighborhoods": [],
    }
    main.books_to_ui_shape = lambda raw, _k: list(raw)  # type: ignore[assignment]
    main._cached_spl_trending = lambda: []  # type: ignore[assignment]

    called = {"create": 0}
    main.auth_panel = lambda: None  # type: ignore[assignment]
    main.render_create_account_page = lambda: called.__setitem__("create", called["create"] + 1)  # type: ignore[assignment]

    class _Storage:
        def load_forum_db(self):  # type: ignore[no-untyped-def]
            return {"posts": []}

        def load_user_store(self, _email=None):  # type: ignore[no-untyped-def]
            return {"accounts": {"users": {}}, "books": {}, "clubs": {}, "forum": {}}

    main.get_storage = lambda: _Storage()  # type: ignore[assignment]

    rt.session_state["show_create_account"] = True
    rt.session_state["signed_in"] = False

    main.main()
    assert called["create"] == 1


def test_main_restores_auth_user_from_query() -> None:
    rt = _FakeStreamlitRuntime()
    _install_streamlit(rt)
    main = importlib.import_module("frontend.main")
    importlib.reload(main)

    main.inject_styles = lambda: None  # type: ignore[assignment]
    main.auth_panel = lambda: None  # type: ignore[assignment]
    main.render_tabs = lambda **_kw: None  # type: ignore[assignment]
    main.handle_query_navigation = lambda *_a, **_kw: None  # type: ignore[assignment]

    main.books_service = types.SimpleNamespace(get_trending_books_reviews=lambda _n: [], get_trending_books_spl=lambda _n: [])  # type: ignore[assignment]
    main.events_service = types.SimpleNamespace(get_explore_events=lambda _n: [])  # type: ignore[assignment]
    main.build_ui_bootstrap = lambda _books, _events, _posts: {  # type: ignore[assignment]
        "books": [],
        "books_by_id": {},
        "books_by_source_id": {},
        "clubs": [],
        "genres": [],
        "neighborhoods": [],
    }
    main.books_to_ui_shape = lambda raw, _k: list(raw)  # type: ignore[assignment]
    main._cached_spl_trending = lambda: []  # type: ignore[assignment]

    class _Storage:
        def load_forum_db(self):  # type: ignore[no-untyped-def]
            return {"posts": []}

        def load_user_store(self, _email=None):  # type: ignore[no-untyped-def]
            return {"accounts": {"users": {"u@example.com": {"user_id": "u@example.com"}}}}

    main.get_storage = lambda: _Storage()  # type: ignore[assignment]
    main.get_current_user = lambda _store, _email: {"user_id": "u@example.com", "name": "Restored"}  # type: ignore[assignment]

    rt.query_params["auth_user"] = "u@example.com"
    rt.session_state["signed_in"] = False
    rt.session_state["show_create_account"] = False
    rt.session_state["show_genre_onboarding"] = False

    main.main()
    assert rt.session_state["signed_in"] is True
    assert rt.session_state["user_email"] == "u@example.com"
    assert rt.session_state["user_name"] == "Restored"

