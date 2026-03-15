"""Library tab renderer."""

from __future__ import annotations

import streamlit as st

from frontend.ui.components import render_book_card


def _render_library_tab(
    *, tab, books_by_id: dict[int, dict], current_user: dict | None
) -> None:
    """Render library tab with saved/in-progress/finished lists."""
    with tab:
        st.title("Library")
        if not st.session_state.get("signed_in") or current_user is None:
            st.info("Sign in to see your books.")
        else:
            user_library = current_user["library"]
            ltabs = st.tabs(["Saved", "In Progress", "Finished"])
            for key, ltab in zip(["saved", "in_progress", "finished"], ltabs):
                with ltab:
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
