"""Frontend Streamlit entrypoint."""

from __future__ import annotations


import streamlit as st
import streamlit.components.v1 as components

from backend import config
from backend.data_loader import books_to_ui_shape, build_ui_bootstrap, load_data
from backend.services import books_service, events_service
from backend.services.recommender_service import (
    get_recommended_books_for_user,
    get_recommended_events_for_user,
)
from backend.config import GENRE_DROPDOWN_OPTIONS
from backend.storage import get_storage
from backend.user_store import get_current_user
from frontend.pages.auth import (
    auth_panel,
    render_create_account_page,
    render_genre_onboarding,
)
from frontend.pages.explore_events import _format_when
from frontend.pages.feed import (
    render_book_detail_page,
    resolve_recommended_books,
)
from frontend.pages.forums import (
    _format_comment_time,
    _format_post_time,
    _forum_preview_text,
    build_post_tags,
    can_view_forum_post,
)
from frontend.pages.my_events import _sync_user_clubs_and_save
from frontend.pages.tabs import render_tabs
from frontend.ui.styles import inject_styles

RECOMMENDER_AVAILABLE = True


# Cache TTL (seconds) so carousel/UI clicks don't refetch from AWS every time.
_FEED_CACHE_TTL = 300


@st.cache_data(ttl=_FEED_CACHE_TTL, show_spinner=False)
def _cached_aws_bootstrap():
    """Load bootstrap data from AWS once per TTL; avoids refetch on every rerun (e.g. carousel)."""
    raw_books = books_service.get_trending_books_reviews(50) or []
    try:
        spl = books_service.get_trending_books_spl(50) or []
        seen = {b.get("parent_asin") or b.get("source_id") for b in raw_books}
        for b in spl:
            aid = b.get("parent_asin") or b.get("source_id")
            if aid and aid not in seen:
                seen.add(aid)
                raw_books.append(b)
    except Exception:
        pass
    events = events_service.get_explore_events(36) or []
    storage = get_storage()
    forum_db = storage.load_forum_db()
    forum_posts_raw = forum_db.get("posts", []) if isinstance(forum_db, dict) else []
    return build_ui_bootstrap(raw_books, events, forum_posts_raw)


@st.cache_data(ttl=_FEED_CACHE_TTL, show_spinner=False)
def _cached_spl_trending():
    """Trending list so Feed doesn't refetch on every carousel click.

    - AWS + Local: SPL top-50 checkouts ("Trending in Seattle").
      In local mode this reads `data/processed/spl_top50_checkouts_in_books.json`.
    """
    return books_service.get_trending_books_spl(50) or []


@st.cache_data(ttl=_FEED_CACHE_TTL, show_spinner=False)
def _cached_book_recommendations(user_email: str):
    """Recommendations by user so we don't refetch on every interaction.

    Returns a dict so callers can key further caches off book_updated_at.
    """
    email = (user_email or "").strip().lower()
    storage = get_storage()
    rec = storage.get_user_recommendations(email) or {}
    return {
        "book_updated_at": int(rec.get("book_updated_at") or 0),
        "recommended_books": get_recommended_books_for_user(email),
    }


def init_session(books: list[dict]) -> None:
    """Initialize required Streamlit session-state defaults."""
    st.session_state.setdefault("signed_in", False)
    st.session_state.setdefault("user_email", "")
    st.session_state.setdefault("user_name", "")
    # Local dev can start with an empty dataset if processed files are missing.
    first_id = books[0]["id"] if books else None
    st.session_state.setdefault("selected_book_id", first_id)
    st.session_state.setdefault("selected_book_source_id", None)
    st.session_state.setdefault("show_book_detail_page", False)
    st.session_state.setdefault("selected_forum_post_id", None)
    st.session_state.setdefault("jump_to_forum_detail", False)
    st.session_state.setdefault("jump_to_explore_clubs", False)
    st.session_state.setdefault("show_genre_onboarding", False)
    st.session_state.setdefault("show_create_account", False)
    st.session_state.setdefault("event_saved_message", False)
    st.session_state.setdefault("event_saved_for_club_id", None)
    st.session_state.setdefault("active_tab_after_save", None)
    st.session_state.setdefault("trending_feed_page_index", 0)
    st.session_state.setdefault("recommended_feed_page_index", 0)


def handle_query_navigation(
    books_by_id: dict[int, dict],
    extended_books_by_source_id: dict[str, dict],
    forum_post_ids: set[int],
) -> None:
    """Handle deep-link query params for book detail and forum detail navigation."""
    open_val = st.query_params.get("open")
    source_id_param = (st.query_params.get("source_id") or "").strip()
    # Allow deep-link to any source_id; detail page can fetch metadata on demand.
    if open_val == "detail" and source_id_param:
        st.session_state["selected_book_source_id"] = source_id_param
        st.session_state["selected_book_id"] = None
        st.session_state["show_book_detail_page"] = True
        st.query_params.clear()
        st.rerun()
        return
    book_param = st.query_params.get("book_id")
    if open_val != "detail" or not book_param:
        post_param = st.query_params.get("post_id")
        if open_val == "forum" and post_param:
            try:
                post_id = int(post_param)
            except (TypeError, ValueError):
                return
            if post_id in forum_post_ids:
                st.session_state["selected_forum_post_id"] = post_id
                st.session_state["jump_to_forum_detail"] = True
                st.query_params.clear()
                st.rerun()
        return
    try:
        book_id = int(book_param)
    except (TypeError, ValueError):
        return
    if book_id in books_by_id:
        st.session_state["selected_book_id"] = book_id
        st.session_state["selected_book_source_id"] = None
        st.session_state["show_book_detail_page"] = True
        st.query_params.clear()
        st.rerun()


def main() -> None:
    """Run the Streamlit app entrypoint and render all tabs."""
    st.set_page_config(page_title="Bookish", page_icon="📚", layout="wide")
    inject_styles()
    storage = get_storage()
    # Data from services (AWS) or local files: one place to see where bootstrap data comes from.
    if getattr(config, "IS_AWS", False):
        data = _cached_aws_bootstrap()
    else:
        # Keep local mode close to AWS by using services + build_ui_bootstrap too.
        raw_books = books_service.get_trending_books_reviews(50) or []
        try:
            spl = books_service.get_trending_books_spl(50) or []
            seen = {b.get("parent_asin") or b.get("source_id") for b in raw_books}
            for b in spl:
                aid = b.get("parent_asin") or b.get("source_id")
                if aid and aid not in seen:
                    seen.add(aid)
                    raw_books.append(b)
        except Exception:
            pass
        events = events_service.get_explore_events(36) or []
        forum_db = storage.load_forum_db()
        forum_posts_raw = forum_db.get("posts", []) if isinstance(forum_db, dict) else []
        data = build_ui_bootstrap(raw_books, events, forum_posts_raw)
    books = data["books"]
    books_by_id = data["books_by_id"]
    books_by_source_id = data["books_by_source_id"]
    # Include SPL trending (and other feed lists) so detail page can resolve books from any section
    extended_books_by_source_id = dict(books_by_source_id)
    for b in books_to_ui_shape(_cached_spl_trending(), 50):
        if b.get("source_id"):
            extended_books_by_source_id[str(b["source_id"])] = b
    events = data["clubs"]
    genres = data["genres"]
    neighborhoods = data["neighborhoods"]
    init_session(books)

    st.sidebar.title("Bookish")
    auth_panel()
    if st.session_state.get("show_create_account") and not st.session_state.get("signed_in"):
        render_create_account_page()
        return
    auth_user_from_query = (st.query_params.get("auth_user") or "").strip().lower()
    store = storage.load_user_store(st.session_state.get("user_email", "") or auth_user_from_query)
    users = store["accounts"].get("users") or {}
    if (
        not st.session_state.get("signed_in", False)
        and auth_user_from_query
        and auth_user_from_query in users
    ):
        restored = get_current_user(store, auth_user_from_query)
        if restored:
            st.session_state["signed_in"] = True
            st.session_state["user_email"] = auth_user_from_query
            st.session_state["user_name"] = restored.get(
                "name", auth_user_from_query.split("@")[0]
            )

    current_user = None
    if st.session_state.get("signed_in", False):
        email = st.session_state.get("user_email", "")
        current_user = get_current_user(store, email)
        # If store doesn't have user yet (e.g. eventual consistency after signup), keep them signed in
        # using session state so they aren't prompted to sign in again on the Feed.
        if current_user is None and not st.session_state.get("show_genre_onboarding"):
            current_user = {
                "user_id": email,
                "email": email,
                "name": st.session_state.get("user_name", email.split("@")[0] if email else ""),
                "library": {"in_progress": [], "saved": [], "finished": []},
                "genre_preferences": [],
                "club_ids": [],
                "forum_posts": [],
                "saved_forum_post_ids": [],
            }

    forum_store = storage.load_forum_db()
    forum_posts_data = forum_store["posts"]
    forum_post_ids = {int(p["id"]) for p in forum_posts_data if "id" in p}
    if st.session_state.get("show_genre_onboarding") and st.session_state.get("signed_in"):
        if current_user is None:
            email = st.session_state.get("user_email", "")
            current_user = {
                "user_id": email,
                "email": email,
                "name": st.session_state.get("user_name", email.split("@")[0]),
                "library": {"in_progress": [], "saved": [], "finished": []},
                "genre_preferences": [],
                "club_ids": [],
                "forum_posts": [],
                "saved_forum_post_ids": [],
            }
        render_genre_onboarding(genres=GENRE_DROPDOWN_OPTIONS, current_user=current_user, store=store)
        return
    if st.session_state.get("show_book_detail_page"):
        render_book_detail_page(
            books=books,
            books_by_id=books_by_id,
            extended_books_by_source_id=extended_books_by_source_id,
            current_user=current_user,
            store=store,
            forum_store=forum_store,
            forum_posts_data=forum_posts_data,
            clear_aws_bootstrap_cache=_cached_aws_bootstrap.clear,
            clear_book_recs_cache=_cached_book_recommendations.clear,
        )
        return

    tabs = st.tabs(["Feed", "Explore Events", "My Events", "Library", "Forum"])
    handle_query_navigation(books_by_id, extended_books_by_source_id, forum_post_ids)
    if st.session_state.get("jump_to_forum_detail"):
        components.html(
            (
                "<script>"
                "for(const t of window.parent.document.querySelectorAll('button[role=\"tab\"]')){"
                "if(t.textContent.trim()==='Forum'){t.click();break;}}"
                "</script>"
            ),
            height=0,
        )
        st.session_state["jump_to_forum_detail"] = False
    if st.session_state.get("jump_to_explore_clubs"):
        components.html(
            (
                "<script>"
                "for(const t of window.parent.document.querySelectorAll('button[role=\"tab\"]')){"
                "if(t.textContent.trim()==='Explore Events'){t.click();break;}}"
                "</script>"
            ),
            height=0,
        )
        st.session_state["jump_to_explore_clubs"] = False
    if st.session_state.get("active_tab_after_save") == "explore_events":
        components.html(
            (
                "<script>"
                "for(const t of window.parent.document.querySelectorAll('button[role=\"tab\"]')){"
                "if(t.textContent.trim()==='Explore Events'){t.click();break;}}"
                "</script>"
            ),
            height=0,
        )
    elif st.session_state.get("active_tab_after_save") == "forum":
        components.html(
            (
                "<script>"
                "for(const t of window.parent.document.querySelectorAll('button[role=\"tab\"]')){"
                "if(t.textContent.trim()==='Forum'){t.click();break;}}"
                "</script>"
            ),
            height=0,
        )
    if st.session_state.get("active_tab_after_save"):
        st.session_state["active_tab_after_save"] = None

    render_tabs(
        tabs=tabs,
        books=books,
        genres=genres,
        events=events,
        neighborhoods=neighborhoods,
        current_user=current_user,
        store=store,
        forum_store=forum_store,
        forum_posts_data=forum_posts_data,
        books_by_id=books_by_id,
        books_by_source_id=books_by_source_id,
        extended_books_by_source_id=extended_books_by_source_id,
        recommender_available=RECOMMENDER_AVAILABLE,
        cached_spl_trending=_cached_spl_trending,
        cached_book_recommendations=_cached_book_recommendations,
        resolve_recommended_books=resolve_recommended_books,
        get_recommended_events_for_user=get_recommended_events_for_user,
        format_when=_format_when,
        sync_user_clubs_and_save=_sync_user_clubs_and_save,
        can_view_forum_post=can_view_forum_post,
        build_post_tags=build_post_tags,
        format_post_time=_format_post_time,
        format_comment_time=_format_comment_time,
        forum_preview_text=_forum_preview_text,
        clear_aws_bootstrap_cache=_cached_aws_bootstrap.clear,
        genre_dropdown_options=GENRE_DROPDOWN_OPTIONS,
    )
