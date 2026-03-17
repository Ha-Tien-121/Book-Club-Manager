"""Forum formatting and filtering helpers."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Callable

import streamlit as st

from backend.storage import get_storage
from backend import config
from frontend.ui.components import render_pill_tags


def _format_post_time(post: dict) -> str:
    """Format post time from created_at (Unix) or legacy time_ago."""
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
    """Return whether the current user can view the given forum post."""
    _ = post, current_user
    return True


def build_post_tags(post: dict) -> list[str]:
    """Build displayable tag list from post metadata and explicit tags."""
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
    """Return text truncated for list view."""
    if not text or not text.strip():
        return ""
    if max_chars is None:
        max_chars = getattr(config, "FORUM_PREVIEW_MAX_CHARS", 280)
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars].rstrip() + "…"


def filter_posts_by_tag_query(posts: list[dict], query: str) -> list[dict]:
    """Filter forum posts by matching query against post tags."""
    query = query.strip().lower()
    if not query:
        return posts
    out = []
    for post in posts:
        tag_blob = " ".join(build_post_tags(post)).lower()
        if query in tag_blob:
            out.append(post)
    return out


def _render_forum_tab(
    *,
    tab,
    current_user: dict | None,
    store: dict,
    forum_store: dict,
    forum_posts_data: list[dict],
    can_view_forum_post_fn: Callable[[dict, dict | None], bool] | None = None,
    build_post_tags_fn: Callable[[dict], list[str]] | None = None,
    format_post_time: Callable[[dict], str] | None = None,
    format_comment_time: Callable[[dict], str] | None = None,
    forum_preview_text: Callable[[str], str] | None = None,
    clear_aws_bootstrap_cache: Callable[[], None] | None = None,
    **legacy_kwargs,
) -> None:
    """Render Forum tab and route detail/list states."""
    legacy_can_view_forum_post = legacy_kwargs.pop("can_view_forum_post", None)
    legacy_build_post_tags = legacy_kwargs.pop("build_post_tags", None)
    _ = legacy_kwargs
    if can_view_forum_post_fn is None:
        can_view_forum_post_fn = legacy_can_view_forum_post or (
            lambda _post, _user: True
        )
    if build_post_tags_fn is None:
        build_post_tags_fn = legacy_build_post_tags or (lambda _post: [])
    format_post_time = format_post_time or (lambda _post: "—")
    format_comment_time = format_comment_time or (lambda _comment: "—")
    forum_preview_text = forum_preview_text or (lambda text: text)
    clear_aws_bootstrap_cache = clear_aws_bootstrap_cache or (lambda: None)
    with tab:
        st.title("Forum")
        if st.session_state.pop("forum_form_clear_next", False):
            for k in ("forum_new_title", "forum_new_post", "forum_new_tags"):
                st.session_state.pop(k, None)

        selected_post_id = st.session_state.get("selected_forum_post_id")
        if selected_post_id is not None:
            _render_forum_detail(
                selected_post_id=selected_post_id,
                current_user=current_user,
                store=store,
                forum_store=forum_store,
                forum_posts_data=forum_posts_data,
                can_view_forum_post_fn=can_view_forum_post_fn,
                build_post_tags_fn=build_post_tags_fn,
                format_post_time=format_post_time,
                format_comment_time=format_comment_time,
            )
            return

        _render_forum_create_and_list(
            current_user=current_user,
            forum_store=forum_store,
            forum_posts_data=forum_posts_data,
            build_post_tags_fn=build_post_tags_fn,
            format_post_time=format_post_time,
            forum_preview_text=forum_preview_text,
            clear_aws_bootstrap_cache=clear_aws_bootstrap_cache,
        )


def _render_forum_detail(
    *,
    selected_post_id: int,
    current_user: dict | None,
    store: dict,
    forum_store: dict,
    forum_posts_data: list[dict],
    can_view_forum_post_fn: Callable[[dict, dict | None], bool],
    build_post_tags_fn: Callable[[dict], list[str]],
    format_post_time: Callable[[dict], str],
    format_comment_time: Callable[[dict], str],
) -> None:
    """Render selected forum discussion detail view."""
    selected_post = next(
        (p for p in forum_posts_data if int(p.get("id", -1)) == int(selected_post_id)),
        None,
    )
    if selected_post is None or not can_view_forum_post_fn(selected_post, current_user):
        st.session_state["selected_forum_post_id"] = None
        st.warning("Discussion not found or not accessible.")
        return

    if st.button("← Back to Forum", key="back_forum_posts"):
        st.session_state["selected_forum_post_id"] = None
        st.session_state["active_tab_after_save"] = "forum"
        st.rerun()
    st.markdown(f"## {selected_post['title']}")
    st.caption(f"{selected_post.get('author', 'User')} | {format_post_time(selected_post)}")
    render_pill_tags(build_post_tags_fn(selected_post))
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
                selected_post["likes"] = max(0, int(selected_post.get("likes", 0)) - 1)
            else:
                selected_post.setdefault("liked_by", []).append(email)
                selected_post["likes"] = int(selected_post.get("likes", 0)) + 1
            get_storage().save_forum_db(forum_store)
            st.rerun()

        saved_ids = current_user.get("saved_forum_post_ids", [])
        is_saved = int(selected_post["id"]) in {int(pid) for pid in saved_ids}
        if c2.button(
            "Unsave post" if is_saved else "Save post",
            key=f"save_post_{int(selected_post['id'])}",
        ):
            if is_saved:
                current_user["saved_forum_post_ids"] = [
                    pid for pid in saved_ids if int(pid) != int(selected_post["id"])
                ]
            else:
                current_user["saved_forum_post_ids"].append(int(selected_post["id"]))
            get_storage().save_user_forum(store)
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
        st.caption(format_comment_time(comment))
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
                    comment["liked_by"] = [u for u in c_liked_by if u != email]
                    comment["likes"] = max(0, int(comment.get("likes", 0)) - 1)
                else:
                    comment.setdefault("liked_by", []).append(email)
                    comment["likes"] = int(comment.get("likes", 0)) + 1
                get_storage().save_forum_db(forum_store)
                st.session_state["active_tab_after_save"] = "forum"
                st.rerun()
        else:
            st.caption(f"Likes: {int(comment.get('likes', 0))}")
        st.divider()

    if current_user is None:
        st.caption("Sign in to reply to comments.")
        return

    reply_key = f"reply_text_{int(selected_post['id'])}"
    if st.session_state.pop("forum_reply_clear_key", None) == reply_key:
        st.session_state.pop(reply_key, None)
    with st.expander("Write a reply", expanded=False):
        with st.form(f"reply_form_{int(selected_post['id'])}"):
            reply = st.text_area("Your reply", key=reply_key, placeholder="Add your reply...")
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
            get_storage().save_forum_db(forum_store)
            st.session_state["forum_reply_clear_key"] = reply_key
            st.session_state["active_tab_after_save"] = "forum"
            st.rerun()
        else:
            st.warning("Please write a reply before submitting.")


def _render_forum_create_and_list(
    *,
    current_user: dict | None,
    forum_store: dict,
    forum_posts_data: list[dict],
    build_post_tags_fn: Callable[[dict], list[str]] | None = None,
    format_post_time: Callable[[dict], str] | None = None,
    forum_preview_text: Callable[[str], str] | None = None,
    clear_aws_bootstrap_cache: Callable[[], None] | None = None,
    **legacy_kwargs,
) -> None:
    """Render forum create form and post list views."""
    legacy_build_post_tags = legacy_kwargs.pop("build_post_tags", None)
    _ = legacy_kwargs
    if build_post_tags_fn is None:
        build_post_tags_fn = legacy_build_post_tags or (lambda _post: [])
    format_post_time = format_post_time or (lambda _post: "—")
    forum_preview_text = forum_preview_text or (lambda text: text)
    clear_aws_bootstrap_cache = clear_aws_bootstrap_cache or (lambda: None)
    if st.session_state.get("signed_in") and current_user is not None:
        with st.expander("Create a discussion", expanded=False):
            with st.form("new_forum_post"):
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
                get_storage().save_forum_db(forum_store)
                clear_aws_bootstrap_cache()
                st.session_state["forum_form_clear_next"] = True
                st.session_state["active_tab_after_save"] = "forum"
                st.success("Posted to forum.")
                st.rerun()
            else:
                st.warning("Please add both title and post content.")
    else:
        st.caption("Sign in to create and save forum posts.")

    tag_query = st.text_input("Search by tags", placeholder="Search tags...")
    view_col, sort_col = st.columns([3, 1])
    with view_col:
        view = st.radio("View", ["All", "Saved"], horizontal=True)
    with sort_col:
        sort_by = st.selectbox(
            "Sort by",
            ["Newest first", "Oldest first", "Most liked"],
            key="forum_sort_by",
        )
    posts = list(forum_posts_data)
    if view == "Saved":
        if current_user is None:
            posts = []
        else:
            saved_ids = {int(pid) for pid in current_user.get("saved_forum_post_ids", [])}
            posts = [p for p in posts if int(p.get("id", -1)) in saved_ids]
    query = tag_query.strip().lower()
    if query:
        posts = [p for p in posts if query in " ".join(build_post_tags_fn(p)).lower()]
    # Sort: newest (created_at desc), oldest (created_at asc), most liked (likes desc)
    if sort_by == "Newest first":
        posts = sorted(posts, key=lambda p: int(p.get("created_at") or 0), reverse=True)
    elif sort_by == "Oldest first":
        posts = sorted(posts, key=lambda p: int(p.get("created_at") or 0), reverse=False)
    elif sort_by == "Most liked":
        posts = sorted(posts, key=lambda p: int(p.get("likes") or 0), reverse=True)

    # Scrollable list container: filters stay fixed above, list scrolls below
    if not posts:
        st.caption("No discussions match your filters.")
    else:
        with st.container(height=560):
            for post in posts:
                st.markdown(f"### {post['title']}")
                tags = build_post_tags_fn(post)
                st.caption(f"{post['author']} | {format_post_time(post)}")
                render_pill_tags(tags)
                st.write(forum_preview_text(post.get("preview", "")))
                if st.button("Open discussion", key=f"open_forum_post_{int(post['id'])}"):
                    st.session_state["selected_forum_post_id"] = int(post["id"])
                    st.session_state["active_tab_after_save"] = "forum"
                    st.rerun()
                st.divider()
