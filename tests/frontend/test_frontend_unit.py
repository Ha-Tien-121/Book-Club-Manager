from __future__ import annotations

import importlib
import sys
import types
from typing import Any


class _FakeCtx:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeStreamlitRuntime:
    """Very small Streamlit stub for unit-testing page functions."""

    def __init__(self) -> None:
        self.session_state: dict[str, Any] = {}
        self.query_params: dict[str, Any] = {}
        self.sidebar = self

        self._button_returns: dict[str, bool] = {}
        self._form_submit_returns: dict[str, bool] = {}
        self._text_inputs: dict[str, str] = {}
        self._checkbox_returns: dict[str, bool] = {}
        self.rerun_called = 0

    # ---- primitives ----
    def rerun(self) -> None:
        self.rerun_called += 1

    def set_page_config(self, **_kw: Any) -> None:
        return None

    def cache_data(self, **_kw: Any):
        def _decorator(fn):
            fn.clear = lambda: None  # type: ignore[attr-defined]
            return fn

        return _decorator

    # ---- layout / text ----
    def title(self, *_a: Any, **_kw: Any) -> None:
        return None

    def subheader(self, *_a: Any, **_kw: Any) -> None:
        return None

    def caption(self, *_a: Any, **_kw: Any) -> None:
        return None

    def markdown(self, *_a: Any, **_kw: Any) -> None:
        return None

    def success(self, *_a: Any, **_kw: Any) -> None:
        return None

    def error(self, *_a: Any, **_kw: Any) -> None:
        return None

    # ---- inputs ----
    def button(self, label: str, **_kw: Any) -> bool:
        return bool(self._button_returns.get(label, False))

    def text_input(self, label: str, **_kw: Any) -> str:
        return str(self._text_inputs.get(label, ""))

    def checkbox(self, label: str, **_kw: Any) -> bool:
        return bool(self._checkbox_returns.get(label, False))

    def multiselect(self, *_a: Any, **_kw: Any) -> list[str]:
        return []

    def columns(self, n: int, **_kw: Any):
        return [_FakeCtx() for _ in range(int(n))]

    def form(self, _key: str):
        return _FakeCtx()

    def form_submit_button(self, label: str, **_kw: Any) -> bool:
        return bool(self._form_submit_returns.get(label, False))

    def tabs(self, labels: list[str]):
        return [_FakeCtx() for _ in labels]


def _install_fake_streamlit() -> _FakeStreamlit:
    rt = _FakeStreamlitRuntime()

    st_mod = types.ModuleType("streamlit")
    # attach stateful bits
    st_mod.session_state = rt.session_state  # type: ignore[attr-defined]
    st_mod.query_params = rt.query_params  # type: ignore[attr-defined]
    st_mod.sidebar = rt  # type: ignore[attr-defined]

    # attach functions
    st_mod.rerun = rt.rerun  # type: ignore[attr-defined]
    st_mod.set_page_config = rt.set_page_config  # type: ignore[attr-defined]
    st_mod.cache_data = rt.cache_data  # type: ignore[attr-defined]
    st_mod.title = rt.title  # type: ignore[attr-defined]
    st_mod.subheader = rt.subheader  # type: ignore[attr-defined]
    st_mod.caption = rt.caption  # type: ignore[attr-defined]
    st_mod.markdown = rt.markdown  # type: ignore[attr-defined]
    st_mod.success = rt.success  # type: ignore[attr-defined]
    st_mod.error = rt.error  # type: ignore[attr-defined]
    st_mod.button = rt.button  # type: ignore[attr-defined]
    st_mod.text_input = rt.text_input  # type: ignore[attr-defined]
    st_mod.checkbox = rt.checkbox  # type: ignore[attr-defined]
    st_mod.multiselect = rt.multiselect  # type: ignore[attr-defined]
    st_mod.columns = rt.columns  # type: ignore[attr-defined]
    st_mod.form = rt.form  # type: ignore[attr-defined]
    st_mod.form_submit_button = rt.form_submit_button  # type: ignore[attr-defined]
    st_mod.tabs = rt.tabs  # type: ignore[attr-defined]

    # streamlit.components.v1 is used for HTML injection.
    comps_pkg = types.ModuleType("streamlit.components")
    comps_v1 = types.ModuleType("streamlit.components.v1")
    comps_v1.html = lambda *_a, **_kw: None  # type: ignore[attr-defined]
    comps_pkg.v1 = comps_v1  # type: ignore[attr-defined]

    sys.modules["streamlit"] = st_mod
    sys.modules["streamlit.components"] = comps_pkg
    sys.modules["streamlit.components.v1"] = comps_v1
    return rt  # type: ignore[return-value]


def test_init_session_sets_expected_defaults() -> None:
    fake_st = _install_fake_streamlit()
    mod = importlib.import_module("frontend.main")
    importlib.reload(mod)

    mod.init_session([{"id": 123}])
    assert fake_st.session_state["signed_in"] is False
    assert fake_st.session_state["selected_book_id"] == 123
    assert fake_st.session_state["show_book_detail_page"] is False


def test_handle_query_navigation_sets_detail_by_source_id_and_reruns() -> None:
    fake_st = _install_fake_streamlit()
    mod = importlib.import_module("frontend.main")
    importlib.reload(mod)

    fake_st.query_params["open"] = "detail"
    fake_st.query_params["source_id"] = "A1"

    mod.handle_query_navigation(books_by_id={}, extended_books_by_source_id={}, forum_post_ids=set())

    assert fake_st.session_state["selected_book_source_id"] == "A1"
    assert fake_st.session_state["show_book_detail_page"] is True
    assert fake_st.rerun_called == 1


def test_auth_panel_signed_out_create_account_sets_flag_and_reruns() -> None:
    fake_st = _install_fake_streamlit()
    auth_mod = importlib.import_module("frontend.pages.auth")
    importlib.reload(auth_mod)

    fake_st.session_state["signed_in"] = False
    fake_st.session_state["show_create_account"] = False
    fake_st._button_returns["Create account"] = True

    auth_mod.auth_panel()
    assert fake_st.session_state["show_create_account"] is True
    assert fake_st.rerun_called == 1


def test_render_pill_tags_no_tags_is_noop() -> None:
    _install_fake_streamlit()
    comp_mod = importlib.import_module("frontend.ui.components")
    importlib.reload(comp_mod)

    # Should not raise
    comp_mod.render_pill_tags(["", "  "])

