"""Frontend Streamlit entrypoint."""

from __future__ import annotations

from collections import Counter

import streamlit as st
import streamlit.components.v1 as components

from backend.auth_service import update_user_preferences
from backend.data_loader import load_data
from backend.forum_store import load_forum_store, save_forum_store
from backend.recommender_service import get_book_recommendations
from backend.user_store import (
    get_current_user,
    load_user_store,
    save_user_books,
    save_user_clubs,
    save_user_forum,
)
from frontend.pages.auth import auth_panel
from frontend.ui.components import render_book_card, render_book_carousel, render_pill_tags
from frontend.ui.styles import inject_styles

RECOMMENDER_AVAILABLE = True


def init_session(books: list[dict]) -> None:
    """Initialize required Streamlit session-state defaults."""
    st.session_state.setdefault("signed_in", False)
    st.session_state.setdefault("user_email", "")
    st.session_state.setdefault("user_name", "")
    st.session_state.setdefault("selected_book_id", books[0]["id"])
    st.session_state.setdefault("show_book_detail_page", False)
    st.session_state.setdefault("selected_forum_post_id", None)
    st.session_state.setdefault("jump_to_forum_detail", False)
    st.session_state.setdefault("jump_to_explore_clubs", False)
    st.session_state.setdefault("show_genre_onboarding", False)
    st.session_state.setdefault("trending_feed_page_index", 0)
    st.session_state.setdefault("recommended_feed_page_index", 0)


def handle_query_navigation(books_by_id: dict[int, dict], forum_post_ids: set[int]) -> None:
    """Handle deep-link query params for book detail and forum detail navigation."""
    book_param = st.query_params.get("book_id")
    if st.query_params.get("open") != "detail" or not book_param:
        post_param = st.query_params.get("post_id")
        if st.query_params.get("open") == "forum" and post_param:
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
        st.session_state["show_genre_onboarding"] = False
        st.success("Preferences saved. Taking you to the feed.")
        st.rerun()


def can_view_forum_post(post: dict, current_user: dict | None) -> bool:
    """Return whether the current user can view the given forum post."""
    if post.get("visibility") != "club":
        return True
    if current_user is None:
        return False
    club_id = post.get("club_id")
    if club_id is None:
        return bool(post.get("club"))
    return club_id in current_user.get("club_ids", [])


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
        source_id = str(item.get("book_id") or "").strip()
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


def render_book_detail_page(
    *,
    books: list[dict],
    books_by_id: dict[int, dict],
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
    if st.session_state["selected_book_id"] not in books_by_id:
        st.session_state["selected_book_id"] = books[0]["id"]
    book = books_by_id[st.session_state["selected_book_id"]]
    c1, c2 = st.columns([1, 2])
    with c1:
        st.image(book["cover"], width="stretch")
    with c2:
        st.subheader(book["title"])
        st.caption(book["author"])
        st.write(f"Rating: **{book['rating']}** ({book['rating_count']:,})")
        st.write(book["description"])
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
                save_user_books(store)
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
            st.caption(f"{post['author']} | {post.get('time_ago', '')}")
            render_pill_tags(post_tags)
            st.write(post.get("preview", ""))
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

    joined_clubs = [c for c in clubs if c["id"] in current_user.get("club_ids", [])]
    with st.form(f"book_post_form_{book['id']}"):
        st.markdown("#### Start a discussion")
        post_title = st.text_input("Discussion title")
        post_text = st.text_area("Discussion post")
        c1, c2 = st.columns(2)
        visibility = c1.selectbox("Visibility", ["Public", "Club Members"])
        selected_club_name = None
        selected_club_id = None
        if visibility == "Club Members":
            club_options = [f"{c['id']}::{c['name']}" for c in joined_clubs]
            selected_club_name = c2.selectbox(
                "Club",
                [c.split("::", maxsplit=1)[1] for c in club_options] if club_options else ["No joined clubs"],
                disabled=not club_options,
            )
            if club_options:
                selected_club_id = [
                    int(c.split("::", maxsplit=1)[0])
                    for c in club_options
                    if c.split("::", maxsplit=1)[1] == selected_club_name
                ][0]
        custom_tags = st.text_input(
            "Additional tags (comma-separated)",
            placeholder="example: pacing, ending, characters",
        )
        submit_post = st.form_submit_button("Post discussion")

    if submit_post:
        if not post_title.strip() or not post_text.strip():
            st.warning("Please add both title and post content.")
            return
        if visibility == "Club Members" and not joined_clubs:
            st.warning("Join a club first to create club-only posts.")
            return

        tags = [book["title"]]
        if selected_club_name:
            tags.append(selected_club_name)
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
                "club": selected_club_name if visibility == "Club Members" else None,
                "club_id": selected_club_id if visibility == "Club Members" else None,
                "book_id": int(book["id"]),
                "book_title": book["title"],
                "tags": tags,
                "visibility": "club" if visibility == "Club Members" else "public",
                "replies": 0,
                "likes": 0,
                "liked_by": [],
                "time_ago": "just now",
                "preview": post_text.strip(),
                "comments": [],
            },
        )
        forum_store["next_post_id"] = int(forum_store["next_post_id"]) + 1
        save_forum_store(forum_store)
        st.success("Posted discussion for this book.")
        st.rerun()


def main() -> None:
    """Run the Streamlit app entrypoint and render all tabs."""
    st.set_page_config(page_title="Bookish", page_icon="📚", layout="wide")
    inject_styles()
    data = load_data()
    books = data["books"]
    books_by_id = data["books_by_id"]
    books_by_source_id = data["books_by_source_id"]
    clubs = data["clubs"]
    genres = data["genres"]
    neighborhoods = data["neighborhoods"]
    forum_posts = data["forum_posts"]
    init_session(books)

    st.sidebar.title("Bookish")
    auth_panel()
    store = load_user_store()
    users = store["accounts"].get("users") or {}
    auth_user_from_query = (st.query_params.get("auth_user") or "").strip().lower()
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
        if current_user is None:
            st.session_state["signed_in"] = False
            st.session_state["user_email"] = ""
            st.session_state["user_name"] = ""
            st.rerun()

    forum_store = load_forum_store(forum_posts)
    forum_posts_data = forum_store["posts"]
    forum_post_ids = {int(p["id"]) for p in forum_posts_data if "id" in p}
    if st.session_state.get("show_genre_onboarding") and current_user is not None:
        render_genre_onboarding(genres=genres, current_user=current_user, store=store)
        return
    if st.session_state.get("show_book_detail_page"):
        render_book_detail_page(
            books=books,
            books_by_id=books_by_id,
            clubs=clubs,
            current_user=current_user,
            store=store,
            forum_store=forum_store,
            forum_posts_data=forum_posts_data,
        )
        return

    tabs = st.tabs(["Feed", "Explore Clubs", "My Clubs", "Library", "Forum"])
    handle_query_navigation(books_by_id, forum_post_ids)
    if st.session_state.get("jump_to_forum_detail"):
        components.html(
            """<script>for(const t of window.parent.document.querySelectorAll('button[role="tab"]')){if(t.textContent.trim()==="Forum"){t.click();break;}}</script>""",
            height=0,
        )
        st.session_state["jump_to_forum_detail"] = False
    if st.session_state.get("jump_to_explore_clubs"):
        components.html(
            """<script>for(const t of window.parent.document.querySelectorAll('button[role="tab"]')){if(t.textContent.trim()==="Explore Clubs"){t.click();break;}}</script>""",
            height=0,
        )
        st.session_state["jump_to_explore_clubs"] = False

    with tabs[0]:
        st.title("Discover your next read")
        selected_genres = st.multiselect("Filter by genre", genres, key="feed_genre_filter")
        filtered_books = [
            b
            for b in books
            if not selected_genres or any(g in selected_genres for g in b["genres"])
        ]
        st.subheader("Trending in Seattle")
        trending_source = filtered_books if selected_genres else books
        trending = sorted(trending_source, key=lambda b: b["rating_count"], reverse=True)
        if trending:
            render_book_carousel(
                section_key="trending_feed",
                books=trending,
                cards_per_page=4,
                key_prefix="trend",
                auth_user=st.session_state.get("user_email", ""),
            )
        else:
            st.caption("No trending books match this genre filter.")

        st.subheader("Recommended for you")
        recommendation_rows: list[dict] = []
        if RECOMMENDER_AVAILABLE:
            try:
                user_email = (
                    st.session_state.get("user_email", "")
                    if st.session_state.get("signed_in") and current_user is not None
                    else ""
                )
                recommendation_rows = get_book_recommendations(user_email)
            except (RuntimeError, ValueError, KeyError):
                recommendation_rows = []
        fallback_books = sorted(filtered_books, key=lambda b: b["rating_count"], reverse=True)
        recommended_books = resolve_recommended_books(
            recommendations=recommendation_rows,
            books_by_source_id=books_by_source_id,
            selected_genres=selected_genres,
            fallback_books=fallback_books,
            top_k=max(10, len(filtered_books)),
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

        st.subheader("Suggested book clubs")
        clubs_source = clubs
        if selected_genres:
            allowed = {g.lower() for g in selected_genres}
            clubs_source = [
                c for c in clubs_source if c.get("genre", "").lower() in allowed
            ]
        top_clubs = clubs_source[:5]
        if not top_clubs:
            st.caption("No suggested clubs for this filter.")
        for club in top_clubs:
            st.markdown(f"**{club['name']}**")
            st.caption(f"{club.get('genre', 'General')} | {club.get('location', 'Seattle, WA')}")
            desc = club.get("description", "") or ""
            st.write(desc[:180] + ("..." if len(desc) > 180 else ""))
            if club.get("external_link"):
                st.link_button("Open club", club["external_link"], use_container_width=False)
            if st.session_state.get("signed_in") and current_user is not None:
                joined = int(club["id"]) in {
                    int(cid) for cid in current_user.get("club_ids", [])
                }
                if joined:
                    st.success("Joined")
                elif st.button("Join Club", key=f"feed_join_club_{club['id']}"):
                    current_user["club_ids"].append(club["id"])
                    save_user_clubs(store)
                    st.rerun()
            else:
                st.caption("Sign in to join clubs.")
            st.divider()
        if st.button("See More Clubs", key="see_more_clubs_feed"):
            st.session_state["jump_to_explore_clubs"] = True
            st.rerun()

    with tabs[1]:
        st.title("Explore Clubs")
        search = st.text_input("Search clubs")
        gfilter = st.selectbox("Genre", ["All"] + genres)
        nfilter = st.selectbox("Neighborhood", ["All"] + neighborhoods)
        filtered_clubs = clubs
        if search.strip():
            q = search.strip().lower()
            filtered_clubs = [
                c for c in filtered_clubs if q in c["name"].lower() or q in c["description"].lower()
            ]
        if gfilter != "All":
            filtered_clubs = [c for c in filtered_clubs if c["genre"] == gfilter]
        if nfilter != "All":
            filtered_clubs = [c for c in filtered_clubs if nfilter.lower() in c["location"].lower()]
        for club in filtered_clubs:
            st.subheader(club["name"])
            st.caption(f"{club['genre']} | {club['location']}")
            summary = club["description"][:280] + ("..." if len(club["description"]) > 280 else "")
            st.write(summary)
            st.write(f"Meetings: {club['meeting_day']} at {club['meeting_time']}")
            if club.get("external_link"):
                st.link_button("Open club listing", club["external_link"], use_container_width=False)
            if st.session_state.get("signed_in") and current_user is not None:
                joined = club["id"] in current_user["club_ids"]
                if joined:
                    st.success("Joined")
                elif st.button("Join club", key=f"join_club_{club['id']}"):
                    current_user["club_ids"].append(club["id"])
                    save_user_clubs(store)
                    st.rerun()
            else:
                st.caption("Sign in to join clubs.")
            st.divider()

    with tabs[2]:
        st.title("My Clubs")
        if not st.session_state.get("signed_in") or current_user is None:
            st.info("Sign in to see your clubs.")
        else:
            for club in [c for c in clubs if c["id"] in current_user.get("club_ids", [])]:
                st.subheader(club["name"])
                st.caption(f"{club.get('location', '')}")
                c1, c2 = st.columns([1, 1])
                if c1.button("Details", key=f"details_club_{club['id']}"):
                    toggle_key = f"show_club_details_{club['id']}"
                    st.session_state[toggle_key] = not st.session_state.get(toggle_key, False)
                    st.rerun()
                if c2.button("Remove Club", key=f"remove_club_{club['id']}"):
                    current_user["club_ids"] = [
                        cid for cid in current_user.get("club_ids", []) if int(cid) != int(club["id"])
                    ]
                    save_user_clubs(store)
                    st.rerun()
                if st.session_state.get(f"show_club_details_{club['id']}", False):
                    st.markdown(f"**Genre:** {club.get('genre', 'General')}")
                    st.markdown(f"**Meeting:** {club.get('meeting_day', 'TBD')} at {club.get('meeting_time', 'TBD')}")
                    st.write(club.get("description", ""))
                st.divider()
            if not current_user.get("club_ids"):
                st.info("You have not joined any clubs yet.")

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
                    st.rerun()
                st.markdown(f"## {selected_post['title']}")
                st.caption(
                    f"{selected_post.get('author', 'User')} | {selected_post.get('time_ago', '')}"
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
                        save_forum_store(forum_store)
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
                        save_user_forum(store)
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
                            save_forum_store(forum_store)
                            st.rerun()
                    else:
                        st.caption(f"Likes: {int(comment.get('likes', 0))}")
                    st.divider()

                if current_user is not None:
                    with st.form(f"reply_form_{int(selected_post['id'])}"):
                        reply = st.text_area("Write a reply")
                        submit_reply = st.form_submit_button("Reply")
                    if submit_reply:
                        if reply.strip():
                            selected_post.setdefault("comments", []).append(
                                {
                                    "author": st.session_state.get("user_name", "User"),
                                    "text": reply.strip(),
                                    "likes": 0,
                                    "liked_by": [],
                                }
                            )
                            selected_post["replies"] = len(selected_post.get("comments", []))
                            save_forum_store(forum_store)
                            st.rerun()
                        else:
                            st.warning("Please write a reply before submitting.")
                else:
                    st.caption("Sign in to reply to comments.")
        else:
            if st.session_state.get("signed_in") and current_user is not None:
                joined_clubs = [c for c in clubs if c["id"] in current_user.get("club_ids", [])]
                with st.form("new_forum_post"):
                    st.subheader("Create a discussion")
                    post_title = st.text_input("Title")
                    post_text = st.text_area("Post")
                    c1, c2 = st.columns(2)
                    visibility = c1.selectbox(
                        "Visibility",
                        ["Public", "Club Members"],
                        help="Public posts are visible to everyone. Club posts are only for one club.",
                    )
                    selected_club_name = None
                    selected_club_id = None
                    if visibility == "Club Members":
                        club_options = [f"{c['id']}::{c['name']}" for c in joined_clubs]
                        selected_club_name = c2.selectbox(
                            "Club",
                            [c.split("::", maxsplit=1)[1] for c in club_options]
                            if club_options
                            else ["No joined clubs"],
                            disabled=not club_options,
                        )
                        if club_options:
                            selected_club_id = [
                                int(c.split("::", maxsplit=1)[0])
                                for c in club_options
                                if c.split("::", maxsplit=1)[1] == selected_club_name
                            ][0]
                    custom_tags_text = st.text_input(
                        "Additional tags (comma-separated)",
                        placeholder="example: mystery, pacing, Seattle",
                    )
                    submitted = st.form_submit_button("Post")
                if submitted:
                    if post_title.strip() and post_text.strip():
                        if visibility == "Club Members" and not joined_clubs:
                            st.warning("Join a club first to create club-only posts.")
                        else:
                            tags = []
                            if visibility == "Club Members" and selected_club_name:
                                tags.append(selected_club_name)
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
                                    "club": selected_club_name
                                    if visibility == "Club Members"
                                    else None,
                                    "club_id": selected_club_id
                                    if visibility == "Club Members"
                                    else None,
                                    "book_id": None,
                                    "book_title": None,
                                    "tags": tags,
                                    "visibility": "club"
                                    if visibility == "Club Members"
                                    else "public",
                                    "replies": 0,
                                    "likes": 0,
                                    "liked_by": [],
                                    "time_ago": "just now",
                                    "preview": post_text.strip(),
                                    "comments": [],
                                },
                            )
                            forum_store["next_post_id"] = int(forum_store["next_post_id"]) + 1
                            save_forum_store(forum_store)
                            st.success("Posted to forum.")
                            st.rerun()
                    else:
                        st.warning("Please add both title and post content.")
            else:
                st.caption("Sign in to create and save forum posts.")

            tag_query = st.text_input("Search by tags", placeholder="Search tags...")
            view = st.radio(
                "View", ["All", "Public", "Club Discussions", "Saved"], horizontal=True
            )
            posts = [p for p in forum_posts_data if can_view_forum_post(p, current_user)]
            if view == "Public":
                posts = [p for p in posts if p.get("visibility") != "club"]
            elif view == "Club Discussions":
                posts = [p for p in posts if p.get("visibility") == "club"]
            elif view == "Saved":
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
                st.caption(f"{post['author']} | {post.get('time_ago','')}")
                render_pill_tags(tags)
                st.write(post.get("preview", ""))
                if st.button("Open discussion", key=f"open_forum_post_{int(post['id'])}"):
                    st.session_state["selected_forum_post_id"] = int(post["id"])
                    st.rerun()
                st.divider()

