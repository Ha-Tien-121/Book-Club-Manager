# Frontend Overview

This frontend is a Streamlit app for Bookish. It handles UI rendering, user interaction, and calls into backend services for data, recommendations, and persistence.

## At a glance

- Entry point: `frontend/main.py`
- Feature pages: `frontend/pages/`
- Shared UI helpers: `frontend/ui/`
- Runtime model: Streamlit reruns the script on interaction and keeps UI state in `st.session_state`

## Folder map

```text
frontend/
  main.py
  pages/
    auth.py
    feed.py
    explore_events.py
    my_events.py
    library.py
    forums.py
    tabs.py
  ui/
    components.py
    styles.py
  examples/
    README.md
```

## How the frontend is structured

- `main.py` orchestrates app startup, data bootstrap, high-level routing, and tab setup.
- `pages/*` contain feature-specific renderers (Feed, Events, Library, Forum, Auth).
- `ui/components.py` contains reusable visual building blocks like cards, carousels, and tag pills.
- `ui/styles.py` applies shared theme and styling.

## Navigation model

- Primary navigation is tab-based: Feed, Explore Events, My Events, Library, Forum.
- Detail views (book and forum post) are state-driven via `st.session_state`.
- Query params support deep-link behavior for opening specific book/forum content.

## Data and backend integration

- Frontend reads data through backend services and storage adapters.
- Cached loaders reduce repeated fetches for feed, recommendations, and event/book bootstrap data.
- User actions (save event, update book status, post/reply in forum) call backend write paths and then rerun to refresh UI.

## If you are new to this code

Start here in order:

1. `frontend/main.py`
2. `frontend/pages/tabs.py`
3. `frontend/pages/feed.py`

This gives the quickest understanding of app flow, page boundaries, and shared UI patterns.