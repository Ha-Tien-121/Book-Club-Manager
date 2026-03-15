"""Reusable UI components."""

from __future__ import annotations

import html
from urllib.parse import quote_plus

import streamlit as st


def render_pill_tags(tags: list[str]) -> None:
    """Render tags using the same pill style as book genres."""
    clean_tags = [str(tag).strip() for tag in tags if str(tag).strip()]
    if not clean_tags:
        return
    st.markdown(
        "".join([f"<span class='pill'>{html.escape(tag)}</span>" for tag in clean_tags]),
        unsafe_allow_html=True,
    )


def render_book_card(
    book: dict,
    key_prefix: str,
    auth_user: str = "",
    show_view_details_button: bool = True,
) -> None:
    """Render book card with clickable metadata and optional detail button."""
    auth_query = f"&auth_user={quote_plus(auth_user)}" if auth_user else ""
    source_id = book.get("source_id")
    if source_id:
        href = f"?open=detail&source_id={quote_plus(str(source_id))}{auth_query}"
    else:
        href = f"?book_id={book['id']}&open=detail{auth_query}"
    card_key = f"{key_prefix}_details_{source_id or book['id']}"
    stats = f"Rating: {book['rating']} ({book['rating_count']:,})"
    st.markdown(
        f'<a href="{href}" target="_self"><img src="{book["cover"]}" '
        f'alt="{html.escape(book["title"])}" '
        "style=\"width:145px;max-width:100%;border-radius:8px;\" /></a>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<a href="{href}" target="_self" '
        "style=\"text-decoration:none;color:inherit;\"><strong>"
        f"{html.escape(book['title'])}</strong></a>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<a href="{href}" target="_self" style="text-decoration:none;color:inherit;">'
        f"{html.escape(book['author'])}</a>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<a href="{href}" target="_self" style="text-decoration:none;color:inherit;">'
        f"{html.escape(stats)}</a>",
        unsafe_allow_html=True,
    )
    render_pill_tags(book.get("genres", []))
    if show_view_details_button and st.button("View details", key=card_key):
        if source_id:
            st.session_state["selected_book_source_id"] = str(source_id)
            st.session_state["selected_book_id"] = None
        else:
            st.session_state["selected_book_id"] = book["id"]
            st.session_state["selected_book_source_id"] = None
        st.session_state["show_book_detail_page"] = True
        st.rerun()


def render_book_carousel(
    section_key: str,
    books: list[dict],
    cards_per_page: int,
    key_prefix: str,
    auth_user: str = "",
) -> None:
    """Render paged book cards with previous/next carousel controls."""
    if not books:
        return
    total_pages = max(1, (len(books) + cards_per_page - 1) // cards_per_page)
    page_state_key = f"{section_key}_page_index"
    st.session_state.setdefault(page_state_key, 0)
    current_page = int(st.session_state[page_state_key])
    current_page = max(0, min(current_page, total_pages - 1))
    st.session_state[page_state_key] = current_page
    start = current_page * cards_per_page
    page_books = books[start : start + cards_per_page]
    side_left, center_cards, side_right = st.columns(
        [1, 12, 1], vertical_alignment="center"
    )
    with side_left:
        st.markdown("<div style='height:220px;'></div>", unsafe_allow_html=True)
        if st.button("◀", key=f"{section_key}_prev", disabled=current_page <= 0):
            st.session_state[page_state_key] = max(0, current_page - 1)
            st.rerun()
    with center_cards:
        cols = st.columns(cards_per_page)
        for i, book in enumerate(page_books):
            with cols[i]:
                render_book_card(book, f"{key_prefix}_{current_page}_{i}", auth_user=auth_user)
    with side_right:
        st.markdown("<div style='height:220px;'></div>", unsafe_allow_html=True)
        if st.button(
            "▶", key=f"{section_key}_next", disabled=current_page >= total_pages - 1
        ):
            st.session_state[page_state_key] = min(total_pages - 1, current_page + 1)
            st.rerun()
