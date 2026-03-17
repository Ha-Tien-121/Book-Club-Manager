"""Library tab renderer."""

from __future__ import annotations

import streamlit as st

from backend.storage import get_book_details as storage_get_book_details
from frontend.ui.components import render_book_card


def _resolve_library_book(
    bid: int | str,
    books_by_id: dict[int, dict],
    books_by_source_id: dict[str, dict] | None,
) -> dict | None:
    """Resolve a library shelf entry (source_id str or numeric id) to a book dict.

    Tries, in order:
    - books_by_source_id[bid] when bid is a non-numeric string (parent_asin/source_id)
    - books_by_id[int(bid)] when bid is an int or numeric string
    - Fallback: fetch full details from storage_get_book_details when bid looks like a parent_asin.
    """
    # 1) Direct by source_id (parent_asin)
    if books_by_source_id is not None and isinstance(bid, str) and bid in books_by_source_id:
        return books_by_source_id[bid]

    # 2) Numeric id into books_by_id
    if isinstance(bid, int) and bid in books_by_id:
        return books_by_id[bid]
    if isinstance(bid, str) and bid.isdigit():
        num = int(bid)
        if num in books_by_id:
            return books_by_id[num]

    # 3) Fallback: treat bid as parent_asin and fetch details from storage (AWS S3 parquet/Dynamo).
    if isinstance(bid, str) and bid and not bid.isdigit():
        try:
            detail = storage_get_book_details(bid)
        except (RuntimeError, ValueError, TypeError, KeyError, OSError):
            detail = None
        if detail:
            # Normalize to the same UI shape render_book_card expects.
            return {
                "id": detail.get("id") or detail.get("internal_id") or 0,
                "source_id": detail.get("parent_asin") or bid,
                "title": detail.get("title") or "",
                "author": detail.get("author_name") or detail.get("author") or "",
                "genres": detail.get("genres") or detail.get("categories") or [],
                "cover": detail.get("image_url") or detail.get("cover") or None,
                "average_rating": detail.get("average_rating"),
                "rating_count": detail.get("rating_count"),
                "description": detail.get("description"),
            }

    return None


def _render_library_tab(
    *,
    tab,
    books_by_id: dict[int, dict],
    books_by_source_id: dict[str, dict] | None,
    current_user: dict | None,
) -> None:
    """Render library tab with saved/in-progress/finished lists."""
    books_by_source_id = books_by_source_id or {}
    with tab:
        st.title("Library")
        if not st.session_state.get("signed_in") or current_user is None:
            st.info("Sign in to see your books.")
        else:
            user_library = current_user["library"]
            ltabs = st.tabs(["Saved", "In Progress", "Finished"])
            for key, ltab in zip(["saved", "in_progress", "finished"], ltabs):
                with ltab:
                    shelf_list = user_library.get(key) or []
                    books_in_shelf = [
                        _resolve_library_book(bid, books_by_id, books_by_source_id)
                        for bid in shelf_list
                    ]
                    books_in_shelf = [b for b in books_in_shelf if b is not None]
                    if not books_in_shelf:
                        st.caption("No books in this list yet.")
                        continue
                    cols = st.columns(3)
                    for i, book in enumerate(books_in_shelf):
                        with cols[i % 3]:
                            render_book_card(
                                book,
                                key_prefix=f"lib_{key}_{i}",
                                auth_user=st.session_state.get("user_email", ""),
                                show_view_details_button=True,
                            )
