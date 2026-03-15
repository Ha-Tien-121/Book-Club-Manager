"""Helpers for Explore Events page."""

from __future__ import annotations

from datetime import datetime

import streamlit as st

from frontend.ui.components import render_pill_tags


def _format_when(event: dict) -> str:
    """Format event schedule line from start_iso or meeting fallback fields."""
    start_iso = event.get("start_iso") or ""
    if start_iso:
        try:
            if "T" in start_iso:
                dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(start_iso[:10], "%Y-%m-%d")
            when = dt.strftime("%A, %b %d, %Y")
            if "T" in start_iso:
                when += (
                    dt.strftime(", %I:%M %p").lstrip("0")
                    or dt.strftime(", %I:%M %p")
                )
            return when
        except (ValueError, TypeError):
            pass
    return f"{event.get('meeting_day', 'TBD')}, {event.get('meeting_time', 'TBD')}"


def _render_explore_events_tab(
    *,
    tab,
    events: list[dict],
    neighborhoods: list[str],
    current_user: dict | None,
    store: dict,
    format_when,
    sync_user_clubs_and_save,
) -> None:
    """Render Explore Events tab with city and tag filters."""
    with tab:
        st.title("Explore Events")
        nfilter = st.selectbox("City", ["All"] + neighborhoods, key="explore_neighborhood")
        all_event_tags = sorted(
            {
                str(tag).strip()
                for event in events
                for tag in (event.get("tags") or [])
                if str(tag).strip()
            }
        )
        selected_event_tags = st.multiselect(
            "Filter by genre tags",
            options=all_event_tags,
            key="explore_event_genre_tags",
        )
        filtered_events = events
        if nfilter != "All":
            filtered_events = [
                e for e in filtered_events if nfilter.lower() in e["location"].lower()
            ]
        if selected_event_tags:
            selected_tag_lc = {t.lower() for t in selected_event_tags}
            filtered_events = [
                e
                for e in filtered_events
                if selected_tag_lc.intersection(
                    {
                        str(tag).strip().lower()
                        for tag in (e.get("tags") or [])
                        if str(tag).strip()
                    }
                )
            ]
        if not filtered_events:
            st.info("No events matching your filters.")
        else:
            # Scrollable list container: filters stay fixed above, events list scrolls below
            with st.container(height=560):
                for event in filtered_events:
                    st.subheader(event["name"])
                    st.caption(event.get("location", "Seattle, WA"))
                    desc = event.get("description") or ""
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
                        elif st.button("Save event", key=f"join_club_{eid}"):
                            current_user.setdefault("club_ids", [])
                            if eid not in current_user["club_ids"]:
                                current_user["club_ids"].append(eid)
                            sync_user_clubs_and_save(store, current_user)
                            st.session_state["active_tab_after_save"] = "explore_events"
                            st.rerun()
                    else:
                        st.caption("Sign in to save events.")
                    st.divider()
