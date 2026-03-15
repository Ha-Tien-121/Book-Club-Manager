"""Helpers for My Events and event saving state sync."""

from __future__ import annotations

import streamlit as st

from backend.storage import get_storage
from frontend.ui.components import render_pill_tags


def _sync_user_clubs_and_save(store: dict, current_user: dict | None) -> None:
    """Sync current_user club_ids into store for signed-in user and persist."""
    storage = get_storage()
    email = (
        st.session_state.get("user_email")
        or (current_user or {}).get("user_id")
        or ""
    ).strip().lower()
    if not email:
        return
    store.setdefault("clubs", {})[email] = (
        store.get("clubs", {}).get(email) or {"club_ids": []}
    )
    store["clubs"][email]["club_ids"] = list(
        (current_user or {}).get("club_ids", [])
    )
    storage.save_user_clubs(store)


def _render_my_events_tab(
    *,
    tab,
    events: list[dict],
    current_user: dict | None,
    store: dict,
    format_when,
    sync_user_clubs_and_save,
) -> None:
    """Render My Events tab for signed-in users."""
    with tab:
        st.title("My Events")
        if not st.session_state.get("signed_in") or current_user is None:
            st.info("Sign in to see your events.")
        else:
            for event in [e for e in events if e["id"] in current_user.get("club_ids", [])]:
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
                if st.button("Remove event", key=f"remove_club_{event['id']}"):
                    current_user["club_ids"] = [
                        cid
                        for cid in current_user.get("club_ids", [])
                        if int(cid) != int(event["id"])
                    ]
                    sync_user_clubs_and_save(store, current_user)
                    st.rerun()
                st.divider()
            if not current_user.get("club_ids"):
                st.info("You have not saved any events yet.")
