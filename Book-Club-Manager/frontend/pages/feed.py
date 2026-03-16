"""Feed and book-detail helpers."""

from __future__ import annotations

import json
import time
from collections import Counter
from typing import Callable

import streamlit as st

from backend.config import BOOK_DESCRIPTION_PREVIEW_CHARS
from backend.data_loader import books_to_ui_shape
from backend.services import books_service
from backend.services import library_service
from backend.storage import get_book_details as storage_get_book_details
from backend.storage import get_storage
from frontend.pages.forums import (
    _format_post_time,
    _forum_preview_text,
    build_post_tags,
    can_view_forum_post,
)
from frontend.ui.components import render_book_carousel, render_pill_tags


def build_user_recommender_stores(
    current_user: dict | None,
    user_email: str,
    books_by_id: dict[int, dict],
    events: list[dict],
    books_by_source_id: dict[str, dict] | None = None,
) -> tuple[dict, dict, bool]:
    """Build recommender stores from user account activity."""
    if current_user is None or not user_email:
        return {}, {}, False
    books_by_source_id = books_by_source_id or {}
    user_books_read_store: dict[str, list[str]] = {}
    user_genres_store: dict[str, list[dict]] = {}
    has_behavior_data = False
    source_ids: list[str] = []
    for shelf in ("in_progress", "saved", "finished"):
        for bid in current_user.get("library", {}).get(shelf, []):
            # Library may store source_id (str) or numeric id (int).
            if isinstance(bid, str) and not bid.isdigit():
                source_ids.append(bid)
            else:
                book = books_by_id.get(int(bid) if isinstance(bid, str) else bid) if bid is not None else None
                if not book and isinstance(bid, str) and bid in books_by_source_id:
                    book = books_by_source_id[bid]
                if book and book.get("source_id"):
                    source_ids.append(str(book["source_id"]))
    if source_ids:
        user_books_read_store[user_email] = list(dict.fromkeys(source_ids))
        has_behavior_data = True
    genre_counts: Counter = Counter()
    joined_ids = {str(cid).strip() for cid in current_user.get("club_ids", [])}
    for event in events:
        eid = str(event.get("event_id") or event.get("id", "")).strip()
        if not eid or eid not in joined_ids:
            continue
        genre = str(event.get("genre") or "").strip()
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


def _render_feed_tab(
    *,
    tab,
    books: list[dict],
    genres: list[str],
    events: list[dict],
    current_user: dict | None,
    store: dict,
    books_by_source_id: dict[str, dict],
    recommender_available: bool,
    cached_spl_trending: Callable[[], list[dict]],
    cached_book_recommendations: Callable[[str], list[dict]],
    resolve_recommended_books: Callable[..., list[dict]],
    get_recommended_events_for_user: Callable[[str], list[dict]],
    format_when: Callable[[dict], str],
    sync_user_clubs_and_save: Callable[[dict, dict | None], None],
    genre_dropdown_options: list[str],
) -> None:
    """Render Feed tab sections (trending, recommended, and suggested events)."""
    with tab:
        st.title("Discover your next read")
        selected_genres = st.multiselect(
            "Filter by genre",
            options=sorted(set(genres) if genres else set(genre_dropdown_options)),
            key="feed_genre_filter",
        )
        filtered_books = [
            b
            for b in books
            if not selected_genres or any(g in selected_genres for g in b["genres"])
        ]
        st.subheader("Trending in Seattle")
        spl_raw = cached_spl_trending()
        spl_books = books_to_ui_shape(spl_raw, 50)
        trending = [
            b
            for b in spl_books
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
        if recommender_available:
            try:
                user_email = (
                    st.session_state.get("user_email", "")
                    if st.session_state.get("signed_in") and current_user is not None
                    else ""
                )
                recommendation_rows = cached_book_recommendations(user_email)
            except (RuntimeError, ValueError, KeyError):
                recommendation_rows = []
        fallback_books = sorted(filtered_books, key=lambda b: b["rating_count"], reverse=True)
        recommended_books = resolve_recommended_books(
            recommendations=recommendation_rows,
            books_by_source_id=books_by_source_id,
            selected_genres=selected_genres,
            fallback_books=fallback_books,
            top_k=50,
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
        events_source = events
        if selected_genres:
            allowed = {g.lower() for g in selected_genres}
            events_source = [
                e for e in events_source if e.get("genre", "").lower() in allowed
            ]
        top_events = events_source[:5]
        if st.session_state.get("signed_in") and current_user is not None:
            try:
                rec_events = get_recommended_events_for_user(
                    st.session_state.get("user_email") or current_user.get("user_id")
                )
                if rec_events:
                    event_id_to_event = {
                        str(e.get("event_id")): e
                        for e in events
                        if e.get("event_id")
                    }
                    ordered = [
                        event_id_to_event[e["event_id"]]
                        for e in rec_events
                        if e.get("event_id") in event_id_to_event
                    ]
                    if ordered:
                        top_events = ordered[:5]
            except Exception:
                pass
        if not top_events:
            st.caption("No suggested events.")
        for event in top_events:
            st.subheader(event["name"])
            st.caption(event.get("location", "Seattle, WA"))
            desc = event.get("description", "") or ""
            summary = desc[:280] + ("..." if len(desc) > 280 else "")
            st.write(summary)
            st.write(f"**When:** {format_when(event)}")
            event_tags = event.get("tags") or [event.get("genre", "General")]
            if event_tags:
                render_pill_tags(event_tags)
            if event.get("external_link"):
                st.link_button(
                    "Open event listing", event["external_link"], use_container_width=False
                )
            if st.session_state.get("signed_in") and current_user is not None:
                eid = str(event.get("event_id") or event.get("id"))
                joined = eid in current_user.get("club_ids", [])
                if joined:
                    st.success("Saved")
                elif st.button("Save event", key=f"feed_join_club_{eid}"):
                    current_user.setdefault("club_ids", [])
                    if eid not in current_user["club_ids"]:
                        current_user["club_ids"].append(eid)
                    sync_user_clubs_and_save(store, current_user)
                    st.session_state["active_tab_after_save"] = "feed"
                    st.rerun()
            else:
                st.caption("Sign in to save events.")
            st.divider()
        if st.button("See More Events", key="see_more_clubs_feed"):
            st.session_state["jump_to_explore_clubs"] = True
            st.rerun()


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
        source_id = str(
            item.get("book_id")
            or item.get("parent_asin")
            or item.get("source_id")
            or ""
        ).strip()
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
    """Normalize description from detail response to displayable text."""
    if not detail:
        return ""
    raw = detail.get("description")
    if raw is None:
        return ""
    text = ""
    if isinstance(raw, list):
        parts = [str(p).strip() for p in raw if p is not None and str(p).strip()]
        text = " ".join(parts).strip() or ""
    elif isinstance(raw, str):
        s = raw.strip()
        if s.startswith("[") and "]" in s:
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    parts = [
                        str(p).strip()
                        for p in parsed
                        if p is not None and str(p).strip()
                    ]
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
    for prefix in ("Amazon.com Review ", "An Amazon Best Book of "):
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
    if ":" in text[:30]:
        idx = text.index(":", 0, 30) + 1
        rest = text[idx:].strip()
        if len(rest) > 20:
            text = rest
    if (text or "").strip() == "[]":
        return ""
    return text or ""


def render_book_detail_page(
    *,
    books: list[dict],
    books_by_id: dict[int, dict],
    extended_books_by_source_id: dict[str, dict],
    current_user: dict | None,
    store: dict,
    forum_store: dict,
    forum_posts_data: list[dict],
    clear_aws_bootstrap_cache: Callable[[], None] | None = None,
) -> None:
    """Render Book Detail page with library and related forum discussions."""
    if st.button("← Back to Feed"):
        st.session_state["show_book_detail_page"] = False
        st.rerun()
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

    st.title(book.get("title") or "Book Detail")
    source_id = (book.get("source_id") or "").strip()
    if source_id and not source_id.startswith("_idx_"):
        detail = None
        try:
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
                book["cover"] = detail.get("images") or book.get(
                    "cover", "https://placehold.co/220x330?text=Book"
                )

    c1, c2 = st.columns([1, 2])
    with c1:
        st.image(book["cover"], use_container_width=True)
    with c2:
        st.caption(book["author"])
        if book.get("genres"):
            render_pill_tags(book["genres"])
        st.write(f"Rating: **{book['rating']}** ({book['rating_count']:,})")
        raw_desc = (book.get("description") or "").strip()
        if raw_desc == "[]":
            raw_desc = ""
        desc_text = raw_desc if raw_desc else ""
        if desc_text:
            max_preview = BOOK_DESCRIPTION_PREVIEW_CHARS
            expand_key = f"book_desc_expanded_{book.get('source_id') or book.get('id')}"
            is_expanded = st.session_state.get(expand_key, False)
            if len(desc_text) > max_preview:
                if is_expanded:
                    st.write(desc_text)
                    if st.button("See less", key=f"{expand_key}_btn"):
                        st.session_state[expand_key] = False
                        st.rerun()
                else:
                    st.write(desc_text[:max_preview].rstrip() + "…")
                    if st.button("See more", key=f"{expand_key}_btn"):
                        st.session_state[expand_key] = True
                        st.rerun()
            else:
                st.write(desc_text)
        # Library: show current status and one control to add or update
        if not st.session_state.get("signed_in", False):
            st.caption("Sign in to add books to your library.")
        else:
            library = (current_user or {}).get("library") or {}
            # Use stable id for library: source_id (parent_asin) when real, else numeric id.
            source_id = (book.get("source_id") or "").strip()
            use_source_id = source_id and not source_id.startswith("_idx_")
            library_book_id = source_id if use_source_id else book.get("id")
            # Check status: shelf may contain either source_id (str) or legacy numeric id (int).
            current_status = None
            for shelf_key, label in [
                ("saved", "Saved"),
                ("in_progress", "In Progress"),
                ("finished", "Finished"),
            ]:
                shelf_list = library.get(shelf_key) or []
                if library_book_id in shelf_list:
                    current_status = label
                    break
                if not use_source_id and book.get("id") in shelf_list:
                    current_status = label
                    break
            options = ["Not in library", "Saved", "In Progress", "Finished"]
            default_idx = 0 if current_status is None else options.index(current_status)
            st.caption(
                "In your library: **" + (current_status or "not saved yet") + "**"
                if current_status
                else "Add this book to your library or choose a status."
            )
            select_key = f"book_lib_status_{library_book_id}"

            def _apply_library_status():
                new_status = st.session_state.get(select_key)
                if new_status is None or current_user is None:
                    return
                user_id = (current_user.get("user_id") or current_user.get("email") or st.session_state.get("user_email", "") or "").strip().lower()
                if not user_id:
                    return
                try:
                    if new_status == "Not in library":
                        library_service.remove_book_from_library(user_id, library_book_id)
                    else:
                        key_map = {"Saved": "saved", "In Progress": "in_progress", "Finished": "finished"}
                        shelf = key_map.get(new_status, "saved")
                        genres = (book.get("genres") or []) if isinstance(book.get("genres"), list) else []
                        library_service.add_book_to_library(
                            user_id, library_book_id, shelf, genres_from_book=genres or None
                        )
                    st.session_state["book_lib_last_msg"] = "Library updated." if current_status else "Added to your library."
                except Exception:
                    st.session_state["book_lib_last_msg"] = "Failed to update library."
                # Streamlit reruns the script automatically after this callback; do not call st.rerun() here.

            st.selectbox(
                "Library status",
                options=options,
                index=default_idx,
                key=select_key,
                on_change=_apply_library_status,
            )
            last_msg = st.session_state.pop("book_lib_last_msg", None)
            if last_msg:
                st.success(last_msg)

    st.divider()
    st.subheader("Discussions for this book")
    related_posts: list[dict] = []
    book_title_lower = str(book["title"]).strip().lower()
    for post in forum_posts_data:
        post_book_id = post.get("book_id")
        post_book_title = str(post.get("book_title") or "").strip().lower()
        tag_hit = any(
            book_title_lower == str(t).strip().lower() for t in post.get("tags", [])
        )
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
            if st.button(
                "Open discussion", key=f"open_book_discussion_{int(post['id'])}"
            ):
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
        if clear_aws_bootstrap_cache is not None:
            clear_aws_bootstrap_cache()
        for k in (
            f"{book_form_key}_title",
            f"{book_form_key}_text",
            f"{book_form_key}_tags",
        ):
            st.session_state.pop(k, None)
        st.session_state["active_tab_after_save"] = "forum"
        st.success("Posted discussion for this book.")
        st.rerun()
