"""Frontend Streamlit entrypoint."""

from __future__ import annotations

import time
from collections import Counter
from datetime import datetime

import streamlit as st
import streamlit.components.v1 as components

from backend.services.library_service import update_user_preferences
from backend.data_loader import books_to_ui_shape, build_ui_bootstrap, load_data
from backend.services import books_service
from backend.services.recommender_service import (
    get_book_recommendations,
    get_recommended_events_for_user,
    refresh_and_save_recommendations,
)
from backend.config import BOOK_DESCRIPTION_PREVIEW_CHARS, GENRE_DROPDOWN_OPTIONS
from backend.storage import get_storage, get_book_details as storage_get_book_details
from backend.user_store import get_current_user
from backend.services.auth_service import create_user, get_user
from frontend.pages.auth import auth_panel
from frontend.ui.components import render_book_card, render_book_carousel, render_pill_tags
from frontend.ui.styles import inject_styles

RECOMMENDER_AVAILABLE = True


def _sync_user_clubs_and_save(store: dict, current_user: dict | None) -> None:
    """Sync current_user's club_ids into store for the signed-in user, then persist.
    Ensures saved events appear on My Events even when current_user is a session-built dict."""
    storage = get_storage()
    email = (st.session_state.get("user_email") or (current_user or {}).get("user_id") or "").strip().lower()
    if not email:
        return
    store.setdefault("clubs", {})[email] = store.get("clubs", {}).get(email) or {"club_ids": []}
    store["clubs"][email]["club_ids"] = list((current_user or {}).get("club_ids", []))
    storage.save_user_clubs(store)


def _format_when(club: dict) -> str:
    """Format event when line: use start_iso date if present, else meeting_day/time."""
    start_iso = club.get("start_iso") or ""
    if start_iso:
        try:
            if "T" in start_iso:
                dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(start_iso[:10], "%Y-%m-%d")
            when = dt.strftime("%A, %b %d, %Y")
            if "T" in start_iso:
                when += dt.strftime(", %I:%M %p").lstrip("0") or dt.strftime(", %I:%M %p")
            return when
        except (ValueError, TypeError):
            pass
    return f"{club.get('meeting_day', 'TBD')}, {club.get('meeting_time', 'TBD')}"


# Cache TTL (seconds) so carousel/UI clicks don't refetch from AWS every time.
_FEED_CACHE_TTL = 300


@st.cache_data(ttl=_FEED_CACHE_TTL)
def _cached_aws_bootstrap():
    """Load bootstrap data from AWS once per TTL; avoids refetch on every rerun (e.g. carousel)."""
    from backend.services import events_service
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


@st.cache_data(ttl=_FEED_CACHE_TTL)
def _cached_spl_trending():
    """SPL trending list so Feed doesn't refetch on every carousel click."""
    return books_service.get_trending_books_spl(50) or []


@st.cache_data(ttl=_FEED_CACHE_TTL)
def _cached_book_recommendations(user_email: str):
    """Recommendations by user so we don't refetch on every interaction."""
    return get_book_recommendations(user_email or "", top_k=25)


def init_session(books: list[dict]) -> None:
    """Initialize required Streamlit session-state defaults."""
    st.session_state.setdefault("signed_in", False)
    st.session_state.setdefault("user_email", "")
    st.session_state.setdefault("user_name", "")
    st.session_state.setdefault("selected_book_id", books[0]["id"])
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
    if open_val == "detail" and source_id_param and source_id_param in extended_books_by_source_id:
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


def render_genre_onboarding(genres: list[str], current_user: dict, store: dict) -> None:
    """Show genre preference checkboxes; save to user and return to feed on Save."""
    st.title("Welcome! Choose your favorite genres")
    st.caption("Check the genres you most like to read.")
    current_prefs = set(current_user.get("genre_preferences") or [])
    selected: list[str] = []
    cols = st.columns(3)
    for i, genre in enumerate(sorted(genres)):
        with cols[i % 3]:
            if st.checkbox(
                genre, value=genre in current_prefs, key=f"genre_onboarding_{genre}"
            ):
                selected.append(genre)
    if st.button("Save Preferences", key="save_genre_preferences"):
        update_user_preferences(current_user["user_id"], selected)
        current_user["genre_preferences"] = selected
        try:
            refresh_and_save_recommendations(current_user["user_id"])
        except Exception:
            pass
        st.session_state["show_genre_onboarding"] = False
        st.success("Preferences saved. Taking you to the feed.")
        st.rerun()


def render_create_account_page() -> None:
    """Full-page create account form; Back returns to sign-in."""
    st.title("Create account")
    if st.button("← Back to sign in"):
        st.session_state["show_create_account"] = False
        st.rerun()

    with st.form("create_account_page_form"):
        email = st.text_input("Email", placeholder="you@example.com")
        password = st.text_input("Password", type="password")
        name = st.text_input("Display name (optional)", placeholder="Your name")
        submit = st.form_submit_button("Create account")

    if submit:
        email = (email or "").strip().lower()
        password = (password or "").strip()
        name = (name or "").strip()
        if not email or not password:
            st.error("Email and password are required.")
        else:
            try:
                create_user(email=email, password=password)
                if name:
                    store = get_storage()
                    record = store.get_user_account(email)
                    if record:
                        record["name"] = name
                        store.save_user_account(record)
                st.session_state["signed_in"] = True
                st.session_state["user_email"] = email
                user = get_user(email)
                st.session_state["user_name"] = (user or {}).get("name", name or email.split("@")[0])
                st.session_state["show_create_account"] = False
                st.session_state["show_genre_onboarding"] = True
                st.success("Account created. You're signed in.")
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))


def _format_post_time(post: dict) -> str:
    """Format post time for display from created_at (Unix) or legacy time_ago. No 'just now'."""
    created = post.get("created_at")
    if created is not None:
        try:
            ts = int(created)
            dt = datetime.fromtimestamp(ts)
            return dt.strftime("%b %d, %Y at %I:%M %p")
        except (TypeError, ValueError, OSError):
            pass
    ago = (post.get("time_ago") or "").strip()
    if ago and ago.lower() != "just now":
        return ago
    return "—"


def _format_comment_time(comment: dict) -> str:
    """Format comment time from created_at (Unix)."""
    created = comment.get("created_at")
    if created is not None:
        try:
            ts = int(created)
            dt = datetime.fromtimestamp(ts)
            return dt.strftime("%b %d, %Y at %I:%M %p")
        except (TypeError, ValueError, OSError):
            pass
    return "—"


def can_view_forum_post(post: dict, current_user: dict | None) -> bool:
    """Return whether the current user can view the given forum post. All posts are visible."""
    return True


def build_post_tags(post: dict) -> list[str]:
    """Build displayable tag list from structured post metadata."""
    tags: list[str] = []
    for raw in post.get("tags", []):
        text = str(raw).strip()
        if text and text not in tags:
            tags.append(text)
    for key in ("book_title", "genre", "club"):
        value = str(post.get(key) or "").strip()
        if value and value not in tags:
            tags.append(value)
    return tags


def _forum_preview_text(text: str, max_chars: int | None = None) -> str:
    """Return text truncated for list view; use full text only in Open discussion."""
    if not text or not text.strip():
        return ""
    if max_chars is None:
        from backend import config
        max_chars = getattr(config, "FORUM_PREVIEW_MAX_CHARS", 280)
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars].rstrip() + "…"


def filter_posts_by_tag_query(posts: list[dict], query: str) -> list[dict]:
    """Filter forum posts by matching query against tags."""
    query = query.strip().lower()
    if not query:
        return posts
    out = []
    for post in posts:
        tag_blob = " ".join(build_post_tags(post)).lower()
        if query in tag_blob:
            out.append(post)
    return out


def build_user_recommender_stores(
    current_user: dict | None,
    user_email: str,
    books_by_id: dict[int, dict],
    clubs: list[dict],
) -> tuple[dict, dict, bool]:
    """Build recommender stores from user account activity."""
    if current_user is None or not user_email:
        return {}, {}, False
    user_books_read_store: dict[str, list[str]] = {}
    user_genres_store: dict[str, list[dict]] = {}
    has_behavior_data = False
    source_ids: list[str] = []
    for shelf in ("in_progress", "saved", "finished"):
        for book_id in current_user.get("library", {}).get(shelf, []):
            book = books_by_id.get(book_id)
            if book and book.get("source_id"):
                source_ids.append(str(book["source_id"]))
    if source_ids:
        user_books_read_store[user_email] = list(dict.fromkeys(source_ids))
        has_behavior_data = True
    genre_counts: Counter = Counter()
    joined_ids = {int(cid) for cid in current_user.get("club_ids", [])}
    for club in clubs:
        if int(club.get("id", -1)) not in joined_ids:
            continue
        genre = str(club.get("genre") or "").strip()
        if genre:
            genre_counts[genre] += 1
    if genre_counts:
        ranked_genres = [name for name, _ in genre_counts.most_common(3)]
        user_genres_store[user_email] = [
            {"genre": genre_name, "rank": rank}
            for rank, genre_name in enumerate(ranked_genres, start=1)
        ]
        has_behavior_data = True
    has_forum_data = bool(
        current_user.get("forum_posts") or current_user.get("saved_forum_post_ids")
    )
    if has_forum_data:
        has_behavior_data = True
    return user_genres_store, user_books_read_store, has_behavior_data


def resolve_recommended_books(
    recommendations: list[dict],
    books_by_source_id: dict[str, dict],
    selected_genres: list[str],
    fallback_books: list[dict],
    top_k: int = 10,
) -> list[dict]:
    """Resolve recommender IDs to loaded books and backfill from fallback list."""
    resolved: list[dict] = []
    selected = set(selected_genres)
    for item in recommendations:
        source_id = str(item.get("book_id") or item.get("parent_asin") or item.get("source_id") or "").strip()
        if not source_id:
            continue
        book = books_by_source_id.get(source_id)
        if book is None:
            continue
        if selected and not any(g in selected for g in book.get("genres", [])):
            continue
        resolved.append(book)
        if len(resolved) >= top_k:
            return resolved
    for book in fallback_books:
        if selected and not any(g in selected for g in book.get("genres", [])):
            continue
        if book in resolved:
            continue
        resolved.append(book)
        if len(resolved) >= top_k:
            break
    return resolved


def _description_from_detail(detail: dict) -> str:
    """Normalize description from get_book_detail (list of strings, numpy array, or JSON string) to display text."""
    if not detail:
        return ""
    raw = detail.get("description")
    if raw is None:
        return ""
    text = ""
    # Parquet list columns can come back as list or numpy array; avoid str(ndarray) which looks like "['a' 'b' ...]"
    if isinstance(raw, list):
        parts = [str(p).strip() for p in raw if p is not None and str(p).strip()]
        text = " ".join(parts).strip() or ""
    elif isinstance(raw, str):
        s = raw.strip()
        if s.startswith("[") and "]" in s:
            try:
                import json
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    parts = [str(p).strip() for p in parsed if p is not None and str(p).strip()]
                    text = " ".join(parts).strip() or ""
                else:
                    text = s or ""
            except (ValueError, TypeError):
                text = s or ""
        else:
            text = s or ""
    else:
        try:
            if hasattr(raw, "__iter__"):
                parts = [str(p).strip() for p in raw if p is not None and str(p).strip()]
                text = " ".join(parts).strip() or ""
        except (TypeError, ValueError):
            pass
        if not text:
            text = str(raw).strip() or ""
    # Drop common leading boilerplate so the description starts with the actual review
    for prefix in (
        "Amazon.com Review ",
        "An Amazon Best Book of ",
    ):
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
    # Skip short date-style header like "May 2022:" so the visible text starts with the review body
    if ":" in text[:30]:
        idx = text.index(":", 0, 30) + 1
        rest = text[idx:].strip()
        if len(rest) > 20:
            text = rest
    return text or ""


def render_book_detail_page(
    *,
    books: list[dict],
    books_by_id: dict[int, dict],
    extended_books_by_source_id: dict[str, dict],
    clubs: list[dict],
    current_user: dict | None,
    store: dict,
    forum_store: dict,
    forum_posts_data: list[dict],
) -> None:
    """Render Book Detail page with library and related forum discussions."""
    if st.button("← Back to Feed"):
        st.session_state["show_book_detail_page"] = False
        st.rerun()
    st.title("Book Detail")
    # Resolve by source_id first (feed/trending), then by numeric id (main list)
    book = None
    selected_source_id = st.session_state.get("selected_book_source_id")
    if selected_source_id and selected_source_id in extended_books_by_source_id:
        book = extended_books_by_source_id[selected_source_id]
    if book is None:
        selected_id = st.session_state.get("selected_book_id")
        if selected_id is not None and selected_id in books_by_id:
            book = books_by_id[selected_id]
    if book is None and books:
        st.session_state["selected_book_id"] = books[0]["id"]
        st.session_state["selected_book_source_id"] = None
        book = books_by_id[books[0]["id"]]
    elif book is None:
        st.caption("Book not found.")
        return
    # Fetch full details (including description) from S3 sharded Parquet via get_book_details when we have a real source_id (parent_asin)
    source_id = (book.get("source_id") or "").strip()
    if source_id and not source_id.startswith("_idx_"):
        detail = None
        try:
            # Prefer storage.get_book_details (S3 + shard key) for description; falls back to books_service (which may use DynamoDB)
            detail = storage_get_book_details(source_id)
        except Exception:
            pass
        if not detail:
            try:
                detail = books_service.get_book_detail(source_id)
            except Exception:
                pass
        if detail:
            book = dict(book)
            desc = _description_from_detail(detail)
            if desc:
                book["description"] = desc
            if detail.get("images"):
                book["cover"] = detail.get("images") or book.get("cover", "https://placehold.co/220x330?text=Book")
    c1, c2 = st.columns([1, 2])
    with c1:
        st.image(book["cover"], use_container_width=True)
    with c2:
        st.subheader(book["title"])
        st.caption(book["author"])
        st.write(f"Rating: **{book['rating']}** ({book['rating_count']:,})")
        desc_text = (book.get("description") or "").strip() or "No description available."
        max_preview = BOOK_DESCRIPTION_PREVIEW_CHARS
        if len(desc_text) > max_preview:
            st.write(desc_text[:max_preview].rstrip() + "…")
            with st.expander("See more"):
                st.write(desc_text)
        else:
            st.write(desc_text)
        save_option = st.selectbox(
            "Save to library as",
            ["Saved", "In Progress", "Finished"],
            disabled=not st.session_state.get("signed_in", False),
        )
        if st.button("Update status", disabled=not st.session_state.get("signed_in", False)):
            if current_user is None:
                st.warning("Sign in to save books.")
            else:
                status_key_map = {
                    "Saved": "saved",
                    "In Progress": "in_progress",
                    "Finished": "finished",
                }
                target_key = status_key_map[save_option]
                for key in ["saved", "in_progress", "finished"]:
                    current_user["library"][key] = [
                        bid for bid in current_user["library"][key] if bid != book["id"]
                    ]
                current_user["library"][target_key].append(book["id"])
                get_storage().save_user_books(store)
                st.success(f"Saved to {save_option}.")

    st.divider()
    st.subheader("Discussions for this book")
    related_posts: list[dict] = []
    book_title_lower = str(book["title"]).strip().lower()
    for post in forum_posts_data:
        post_book_id = post.get("book_id")
        post_book_title = str(post.get("book_title") or "").strip().lower()
        tag_hit = any(book_title_lower == str(t).strip().lower() for t in post.get("tags", []))
        id_hit = post_book_id is not None and int(post_book_id) == int(book["id"])
        title_hit = post_book_title == book_title_lower
        if (id_hit or title_hit or tag_hit) and can_view_forum_post(post, current_user):
            related_posts.append(post)

    if related_posts:
        for post in related_posts:
            st.markdown(f"### {post['title']}")
            post_tags = build_post_tags(post)
            st.caption(f"{post['author']} | {_format_post_time(post)}")
            render_pill_tags(post_tags)
            st.write(_forum_preview_text(post.get("preview", "")))
            if st.button("Open discussion", key=f"open_book_discussion_{int(post['id'])}"):
                st.session_state["selected_forum_post_id"] = int(post["id"])
                st.session_state["jump_to_forum_detail"] = True
                st.session_state["show_book_detail_page"] = False
                st.rerun()
            st.divider()
    else:
        st.info("No discussions for this book yet.")

    if not st.session_state.get("signed_in", False) or current_user is None:
        st.caption("Sign in to start a discussion for this book.")
        return

    book_form_key = f"book_post_{book['id']}"
    with st.form(f"book_post_form_{book['id']}"):
        st.markdown("#### Start a discussion")
        post_title = st.text_input("Discussion title", key=f"{book_form_key}_title")
        post_text = st.text_area("Discussion post", key=f"{book_form_key}_text")
        custom_tags = st.text_input(
            "Additional tags (comma-separated)",
            placeholder="example: pacing, ending, characters",
            key=f"{book_form_key}_tags",
        )
        submit_post = st.form_submit_button("Post discussion")

    if submit_post:
        if not post_title.strip() or not post_text.strip():
            st.warning("Please add both title and post content.")
            return

        tags = [book["title"]]
        for raw_tag in custom_tags.split(","):
            tag = raw_tag.strip()
            if tag and tag not in tags:
                tags.append(tag)

        forum_store["posts"].insert(
            0,
            {
                "id": int(forum_store["next_post_id"]),
                "title": post_title.strip(),
                "author": st.session_state.get("user_name", "User"),
                "genre": book["genres"][0] if book.get("genres") else None,
                "book_id": int(book["id"]),
                "book_title": book["title"],
                "tags": tags,
                "replies": 0,
                "likes": 0,
                "liked_by": [],
                "created_at": int(time.time()),
                "preview": post_text.strip(),
                "comments": [],
            },
        )
        forum_store["next_post_id"] = int(forum_store["next_post_id"]) + 1
        get_storage().save_forum_db(forum_store)
        _cached_aws_bootstrap.clear()
        for k in (f"{book_form_key}_title", f"{book_form_key}_text", f"{book_form_key}_tags"):
            st.session_state.pop(k, None)
        st.session_state["active_tab_after_save"] = "forum"
        st.success("Posted discussion for this book.")
        st.rerun()


def main() -> None:
    """Run the Streamlit app entrypoint and render all tabs."""
    st.set_page_config(page_title="Bookish", page_icon="📚", layout="wide")
    inject_styles()
    storage = get_storage()
    # Data from services (AWS) or local files: one place to see where bootstrap data comes from.
    from backend import config
    if getattr(config, "IS_AWS", False):
        data = _cached_aws_bootstrap()
    else:
        data = load_data()
    books = data["books"]
    books_by_id = data["books_by_id"]
    books_by_source_id = data["books_by_source_id"]
    # Include SPL trending (and other feed lists) so detail page can resolve books from any section
    extended_books_by_source_id = dict(books_by_source_id)
    for b in books_to_ui_shape(_cached_spl_trending(), 50):
        if b.get("source_id"):
            extended_books_by_source_id[str(b["source_id"])] = b
    clubs = data["clubs"]
    genres = data["genres"]
    neighborhoods = data["neighborhoods"]
    forum_posts = data["forum_posts"]
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
            clubs=clubs,
            current_user=current_user,
            store=store,
            forum_store=forum_store,
            forum_posts_data=forum_posts_data,
        )
        return

    tabs = st.tabs(["Feed", "Explore Events", "My Events", "Library", "Forum"])
    handle_query_navigation(books_by_id, extended_books_by_source_id, forum_post_ids)
    if st.session_state.get("jump_to_forum_detail"):
        components.html(
            """<script>for(const t of window.parent.document.querySelectorAll('button[role="tab"]')){if(t.textContent.trim()==="Forum"){t.click();break;}}</script>""",
            height=0,
        )
        st.session_state["jump_to_forum_detail"] = False
    if st.session_state.get("jump_to_explore_clubs"):
        components.html(
            """<script>for(const t of window.parent.document.querySelectorAll('button[role="tab"]')){if(t.textContent.trim()==="Explore Events"){t.click();break;}}</script>""",
            height=0,
        )
        st.session_state["jump_to_explore_clubs"] = False
    if st.session_state.get("active_tab_after_save") == "explore_events":
        components.html(
            """<script>for(const t of window.parent.document.querySelectorAll('button[role="tab"]')){if(t.textContent.trim()==="Explore Events"){t.click();break;}}</script>""",
            height=0,
        )
    elif st.session_state.get("active_tab_after_save") == "forum":
        components.html(
            """<script>for(const t of window.parent.document.querySelectorAll('button[role="tab"]')){if(t.textContent.trim()==="Forum"){t.click();break;}}</script>""",
            height=0,
        )
    if st.session_state.get("active_tab_after_save"):
        st.session_state["active_tab_after_save"] = None

    with tabs[0]:
        st.title("Discover your next read")
        selected_genres: list[str] = []
        filtered_books = [
            b
            for b in books
            if not selected_genres or any(g in selected_genres for g in b["genres"])
        ]
        st.subheader("Trending in Seattle")
        spl_raw = _cached_spl_trending()
        spl_books = books_to_ui_shape(spl_raw, 50)
        trending = [
            b for b in spl_books
            if not selected_genres or any(g in selected_genres for g in b.get("genres", []))
        ]
        if trending:
            render_book_carousel(
                section_key="trending_feed",
                books=trending,
                cards_per_page=4,
                key_prefix="trend",
                auth_user=st.session_state.get("user_email", ""),
            )
        else:
            st.caption("No trending books.")

        st.subheader("Recommended for you")
        recommendation_rows: list[dict] = []
        if RECOMMENDER_AVAILABLE:
            try:
                user_email = (
                    st.session_state.get("user_email", "")
                    if st.session_state.get("signed_in") and current_user is not None
                    else ""
                )
                recommendation_rows = _cached_book_recommendations(user_email)
            except (RuntimeError, ValueError, KeyError):
                recommendation_rows = []
        fallback_books = sorted(filtered_books, key=lambda b: b["rating_count"], reverse=True)
        recommended_books = resolve_recommended_books(
            recommendations=recommendation_rows,
            books_by_source_id=books_by_source_id,
            selected_genres=selected_genres,
            fallback_books=fallback_books,
            top_k=25,
        )
        if recommended_books:
            render_book_carousel(
                section_key="recommended_feed",
                books=recommended_books,
                cards_per_page=5,
                key_prefix="rec",
                auth_user=st.session_state.get("user_email", ""),
            )
        else:
            st.caption("No recommendations available yet.")

        st.subheader("Suggested events")
        clubs_source = clubs
        if selected_genres:
            allowed = {g.lower() for g in selected_genres}
            clubs_source = [
                c for c in clubs_source if c.get("genre", "").lower() in allowed
            ]
        # When signed in, use personalized event recommendations (genre-based).
        top_clubs = clubs_source[:5]
        if st.session_state.get("signed_in") and current_user is not None:
            try:
                rec_events = get_recommended_events_for_user(
                    st.session_state.get("user_email") or current_user.get("user_id")
                )
                if rec_events:
                    event_id_to_club = {
                        str(c.get("event_id")): c
                        for c in clubs
                        if c.get("event_id")
                    }
                    ordered = [
                        event_id_to_club[e["event_id"]]
                        for e in rec_events
                        if e.get("event_id") in event_id_to_club
                    ]
                    if ordered:
                        top_clubs = ordered[:5]
            except Exception:
                pass
        if not top_clubs:
            st.caption("No suggested events.")
        for club in top_clubs:
            st.subheader(club["name"])
            st.caption(f"{club.get('genre', 'General')} | {club.get('location', 'Seattle, WA')}")
            desc = club.get("description", "") or ""
            summary = desc[:280] + ("..." if len(desc) > 280 else "")
            st.write(summary)
            st.write(f"**When:** {_format_when(club)}")
            event_tags = club.get("tags") or [club.get("genre", "General")]
            if event_tags:
                render_pill_tags(event_tags)
            if club.get("external_link"):
                st.link_button("Open event listing", club["external_link"], use_container_width=False)
            if st.session_state.get("signed_in") and current_user is not None:
                joined = club["id"] in current_user.get("club_ids", [])
                if joined:
                    st.success("Saved")
                elif st.button("Save event", key=f"feed_join_club_{club['id']}"):
                    current_user["club_ids"].append(club["id"])
                    _sync_user_clubs_and_save(store, current_user)
                    st.session_state["event_saved_for_club_id"] = club["id"]
                    st.session_state["active_tab_after_save"] = "feed"
                    st.rerun()
                if st.session_state.get("event_saved_for_club_id") == club["id"]:
                    st.success("Event saved.")
                    st.session_state["event_saved_for_club_id"] = None
            else:
                st.caption("Sign in to save events.")
            st.divider()
        if st.button("See More Events", key="see_more_clubs_feed"):
            st.session_state["jump_to_explore_clubs"] = True
            st.rerun()

    with tabs[1]:
        st.title("Explore Events")
        nfilter = st.selectbox("City", ["All"] + neighborhoods, key="explore_neighborhood")
        filtered_clubs = clubs
        if nfilter != "All":
            filtered_clubs = [c for c in filtered_clubs if nfilter.lower() in c["location"].lower()]
        if not filtered_clubs:
            st.info("No events matching your filters.")
        for club in filtered_clubs:
            st.subheader(club["name"])
            st.caption(f"{club['genre']} | {club['location']}")
            summary = club["description"][:280] + ("..." if len(club["description"]) > 280 else "")
            st.write(summary)
            st.write(f"**When:** {_format_when(club)}")
            event_tags = club.get("tags") or [club.get("genre", "General")]
            if event_tags:
                render_pill_tags(event_tags)
            if club.get("external_link"):
                st.link_button("Open event listing", club["external_link"], use_container_width=False)
            if st.session_state.get("signed_in") and current_user is not None:
                joined = club["id"] in current_user["club_ids"]
                if joined:
                    st.success("Saved")
                elif st.button("Save event", key=f"join_club_{club['id']}"):
                    current_user["club_ids"].append(club["id"])
                    _sync_user_clubs_and_save(store, current_user)
                    st.session_state["event_saved_for_club_id"] = club["id"]
                    st.session_state["active_tab_after_save"] = "explore_events"
                    st.rerun()
                if st.session_state.get("event_saved_for_club_id") == club["id"]:
                    st.success("Event saved.")
                    st.session_state["event_saved_for_club_id"] = None
            else:
                st.caption("Sign in to save events.")
            st.divider()

    with tabs[2]:
        st.title("My Events")
        if not st.session_state.get("signed_in") or current_user is None:
            st.info("Sign in to see your events.")
        else:
            for club in [c for c in clubs if c["id"] in current_user.get("club_ids", [])]:
                st.subheader(club["name"])
                st.caption(f"{club.get('genre', 'General')} | {club.get('location', 'Seattle, WA')}")
                desc = club.get("description", "") or ""
                summary = desc[:280] + ("..." if len(desc) > 280 else "")
                st.write(summary)
                st.write(f"**When:** {_format_when(club)}")
                event_tags = club.get("tags") or [club.get("genre", "General")]
                if event_tags:
                    render_pill_tags(event_tags)
                if club.get("external_link"):
                    st.link_button("Open event listing", club["external_link"], use_container_width=False)
                if st.button("Remove event", key=f"remove_club_{club['id']}"):
                    current_user["club_ids"] = [
                        cid for cid in current_user.get("club_ids", []) if int(cid) != int(club["id"])
                    ]
                    _sync_user_clubs_and_save(store, current_user)
                    st.rerun()
                st.divider()
            if not current_user.get("club_ids"):
                st.info("You have not saved any events yet.")

    with tabs[3]:
        st.title("Library")
        if not st.session_state.get("signed_in") or current_user is None:
            st.info("Sign in to see your books.")
        else:
            user_library = current_user["library"]
            ltabs = st.tabs(["Saved", "In Progress", "Finished"])
            for key, tab in zip(["saved", "in_progress", "finished"], ltabs):
                with tab:
                    book_ids = [bid for bid in user_library[key] if bid in books_by_id]
                    if not book_ids:
                        st.caption("No books in this list yet.")
                        continue
                    cols = st.columns(3)
                    for i, bid in enumerate(book_ids):
                        with cols[i % 3]:
                            render_book_card(
                                books_by_id[bid],
                                key_prefix=f"lib_{key}_{i}",
                                auth_user=st.session_state.get("user_email", ""),
                                show_view_details_button=True,
                            )

    with tabs[4]:
        st.title("Forum")
        # Clear new-post form after a successful submit (runs every time so it works when returning from other tabs).
        if st.session_state.pop("forum_form_clear_next", False):
            for k in ("forum_new_title", "forum_new_post", "forum_new_tags"):
                st.session_state.pop(k, None)
        selected_post_id = st.session_state.get("selected_forum_post_id")
        if selected_post_id is not None:
            selected_post = next(
                (
                    p
                    for p in forum_posts_data
                    if int(p.get("id", -1)) == int(selected_post_id)
                ),
                None,
            )
            if selected_post is None or not can_view_forum_post(selected_post, current_user):
                st.session_state["selected_forum_post_id"] = None
                st.warning("Discussion not found or not accessible.")
            else:
                if st.button("← Back to Forum", key="back_forum_posts"):
                    st.session_state["selected_forum_post_id"] = None
                    st.session_state["active_tab_after_save"] = "forum"
                    st.rerun()
                st.markdown(f"## {selected_post['title']}")
                st.caption(
                    f"{selected_post.get('author', 'User')} | {_format_post_time(selected_post)}"
                )
                render_pill_tags(build_post_tags(selected_post))
                st.write(selected_post.get("preview", ""))

                c1, c2 = st.columns(2)
                if current_user is not None:
                    email = st.session_state.get("user_email", "")
                    liked_by = selected_post.get("liked_by", [])
                    liked = email in liked_by
                    if c1.button(
                        "Unlike post" if liked else "Like post",
                        key=f"like_post_{int(selected_post['id'])}",
                    ):
                        if liked:
                            selected_post["liked_by"] = [u for u in liked_by if u != email]
                            selected_post["likes"] = max(
                                0, int(selected_post.get("likes", 0)) - 1
                            )
                        else:
                            selected_post.setdefault("liked_by", []).append(email)
                            selected_post["likes"] = int(selected_post.get("likes", 0)) + 1
                        storage.save_forum_db(forum_store)
                        st.rerun()

                    saved_ids = current_user.get("saved_forum_post_ids", [])
                    is_saved = int(selected_post["id"]) in {
                        int(pid) for pid in saved_ids
                    }
                    if c2.button(
                        "Unsave post" if is_saved else "Save post",
                        key=f"save_post_{int(selected_post['id'])}",
                    ):
                        if is_saved:
                            current_user["saved_forum_post_ids"] = [
                                pid
                                for pid in saved_ids
                                if int(pid) != int(selected_post["id"])
                            ]
                        else:
                            current_user["saved_forum_post_ids"].append(
                                int(selected_post["id"])
                            )
                        storage.save_user_forum(store)
                        st.session_state["active_tab_after_save"] = "forum"
                        st.rerun()
                else:
                    c1.caption("Sign in to like posts.")
                    c2.caption("Sign in to save posts.")

                st.caption(
                    f"Likes: {int(selected_post.get('likes', 0))} | Replies: {int(selected_post.get('replies', 0))}"
                )
                st.markdown("#### Comments")
                comments = selected_post.get("comments", [])
                if not comments:
                    st.caption("No comments yet.")
                for idx, comment in enumerate(comments):
                    st.markdown(f"**{comment.get('author', 'User')}**")
                    st.caption(_format_comment_time(comment))
                    st.write(comment.get("text", ""))
                    if current_user is not None:
                        email = st.session_state.get("user_email", "")
                        c_liked_by = comment.get("liked_by", [])
                        c_liked = email in c_liked_by
                        if st.button(
                            f"{'Unlike' if c_liked else 'Like'} comment ({int(comment.get('likes', 0))})",
                            key=f"like_comment_{int(selected_post['id'])}_{idx}",
                        ):
                            if c_liked:
                                comment["liked_by"] = [
                                    u for u in c_liked_by if u != email
                                ]
                                comment["likes"] = max(
                                    0, int(comment.get("likes", 0)) - 1
                                )
                            else:
                                comment.setdefault("liked_by", []).append(email)
                                comment["likes"] = int(comment.get("likes", 0)) + 1
                            storage.save_forum_db(forum_store)
                            st.session_state["active_tab_after_save"] = "forum"
                            st.rerun()
                    else:
                        st.caption(f"Likes: {int(comment.get('likes', 0))}")
                    st.divider()

                if current_user is not None:
                    reply_key = f"reply_text_{int(selected_post['id'])}"
                    # Clear reply box on next run after submit (cannot modify widget key after it's created).
                    if st.session_state.pop("forum_reply_clear_key", None) == reply_key:
                        st.session_state.pop(reply_key, None)
                    with st.form(f"reply_form_{int(selected_post['id'])}"):
                        reply = st.text_area("Write a reply", key=reply_key)
                        submit_reply = st.form_submit_button("Reply")
                    if submit_reply:
                        if reply.strip():
                            selected_post.setdefault("comments", []).append(
                                {
                                    "author": st.session_state.get("user_name", "User"),
                                    "text": reply.strip(),
                                    "likes": 0,
                                    "liked_by": [],
                                    "created_at": int(time.time()),
                                }
                            )
                            selected_post["replies"] = len(selected_post.get("comments", []))
                            storage.save_forum_db(forum_store)
                            st.session_state["forum_reply_clear_key"] = reply_key
                            st.session_state["active_tab_after_save"] = "forum"
                            st.rerun()
                        else:
                            st.warning("Please write a reply before submitting.")
                else:
                    st.caption("Sign in to reply to comments.")
        else:
            if st.session_state.get("signed_in") and current_user is not None:
                with st.form("new_forum_post"):
                    st.subheader("Create a discussion")
                    post_title = st.text_input("Title", key="forum_new_title")
                    post_text = st.text_area("Post", key="forum_new_post")
                    custom_tags_text = st.text_input(
                        "Additional tags (comma-separated)",
                        placeholder="example: mystery, pacing, Seattle",
                        key="forum_new_tags",
                    )
                    submitted = st.form_submit_button("Post")
                if submitted:
                    if post_title.strip() and post_text.strip():
                        tags = []
                        for raw_tag in custom_tags_text.split(","):
                            tag = raw_tag.strip()
                            if tag and tag not in tags:
                                tags.append(tag)
                        forum_store["posts"].insert(
                            0,
                            {
                                "id": int(forum_store["next_post_id"]),
                                "title": post_title.strip(),
                                "author": st.session_state.get("user_name", "User"),
                                "genre": None,
                                "book_id": None,
                                "book_title": None,
                                "tags": tags,
                                "replies": 0,
                                "likes": 0,
                                "liked_by": [],
                                "created_at": int(time.time()),
                                "preview": post_text.strip(),
                                "comments": [],
                            },
                        )
                        forum_store["next_post_id"] = int(forum_store["next_post_id"]) + 1
                        storage.save_forum_db(forum_store)
                        _cached_aws_bootstrap.clear()
                        st.session_state["forum_form_clear_next"] = True
                        st.session_state["active_tab_after_save"] = "forum"
                        st.success("Posted to forum.")
                        st.rerun()
                    else:
                        st.warning("Please add both title and post content.")
            else:
                st.caption("Sign in to create and save forum posts.")

            tag_query = st.text_input("Search by tags", placeholder="Search tags...")
            view = st.radio("View", ["All", "Saved"], horizontal=True)
            posts = list(forum_posts_data)
            if view == "Saved":
                if current_user is None:
                    posts = []
                else:
                    saved_ids = {
                        int(pid) for pid in current_user.get("saved_forum_post_ids", [])
                    }
                    posts = [p for p in posts if int(p.get("id", -1)) in saved_ids]
            posts = filter_posts_by_tag_query(posts, tag_query)
            for post in posts:
                st.markdown(f"### {post['title']}")
                tags = build_post_tags(post)
                st.caption(f"{post['author']} | {_format_post_time(post)}")
                render_pill_tags(tags)
                st.write(_forum_preview_text(post.get("preview", "")))
                if st.button("Open discussion", key=f"open_forum_post_{int(post['id'])}"):
                    st.session_state["selected_forum_post_id"] = int(post["id"])
                    st.session_state["active_tab_after_save"] = "forum"
                    st.rerun()
                st.divider()

