"""Tab orchestration for the main Streamlit UI."""

from __future__ import annotations

from typing import Callable

from .explore_events import _render_explore_events_tab
from .feed import _render_feed_tab
from .forums import _render_forum_tab
from .library import _render_library_tab
from .my_events import _render_my_events_tab


def render_tabs(
    *,
    tabs: list,
    books: list[dict],
    genres: list[str],
    events: list[dict],
    neighborhoods: list[str],
    current_user: dict | None,
    store: dict,
    forum_store: dict,
    forum_posts_data: list[dict],
    books_by_id: dict[int, dict],
    books_by_source_id: dict[str, dict],
    recommender_available: bool,
    cached_spl_trending: Callable[[], list[dict]],
    cached_book_recommendations: Callable[[str], list[dict]],
    resolve_recommended_books: Callable[..., list[dict]],
    get_recommended_events_for_user: Callable[[str], list[dict]],
    format_when: Callable[[dict], str],
    sync_user_clubs_and_save: Callable[[dict, dict | None], None],
    can_view_forum_post: Callable[[dict, dict | None], bool],
    build_post_tags: Callable[[dict], list[str]],
    format_post_time: Callable[[dict], str],
    format_comment_time: Callable[[dict], str],
    forum_preview_text: Callable[[str], str],
    clear_aws_bootstrap_cache: Callable[[], None],
    genre_dropdown_options: list[str],
) -> None:
    """Render all main tabs (Feed, Explore Events, My Events, Library, Forum)."""
    _render_feed_tab(
        tab=tabs[0],
        books=books,
        genres=genres,
        events=events,
        current_user=current_user,
        store=store,
        books_by_source_id=books_by_source_id,
        recommender_available=recommender_available,
        cached_spl_trending=cached_spl_trending,
        cached_book_recommendations=cached_book_recommendations,
        resolve_recommended_books=resolve_recommended_books,
        get_recommended_events_for_user=get_recommended_events_for_user,
        format_when=format_when,
        sync_user_clubs_and_save=sync_user_clubs_and_save,
        genre_dropdown_options=genre_dropdown_options,
    )
    _render_explore_events_tab(
        tab=tabs[1],
        events=events,
        neighborhoods=neighborhoods,
        current_user=current_user,
        store=store,
        format_when=format_when,
        sync_user_clubs_and_save=sync_user_clubs_and_save,
    )
    _render_my_events_tab(
        tab=tabs[2],
        events=events,
        current_user=current_user,
        store=store,
        format_when=format_when,
        sync_user_clubs_and_save=sync_user_clubs_and_save,
    )
    _render_library_tab(tab=tabs[3], books_by_id=books_by_id, current_user=current_user)
    _render_forum_tab(
        tab=tabs[4],
        current_user=current_user,
        store=store,
        forum_store=forum_store,
        forum_posts_data=forum_posts_data,
        can_view_forum_post=can_view_forum_post,
        build_post_tags=build_post_tags,
        format_post_time=format_post_time,
        format_comment_time=format_comment_time,
        forum_preview_text=forum_preview_text,
        clear_aws_bootstrap_cache=clear_aws_bootstrap_cache,
    )
