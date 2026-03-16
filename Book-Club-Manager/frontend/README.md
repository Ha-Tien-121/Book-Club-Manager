## Frontend overview

This `frontend` package contains the Streamlit UI for Book Club Manager. It is organized into:

- **`main.py`**: single Streamlit entrypoint that wires together authentication, navigation, and all page tabs.
- **`pages/`**: feature-specific view logic (feed, library, events, forums, auth).
- **`ui/`**: shared visual components and styles.

The frontend calls into `backend.services.*` modules for all business logic and into `backend.config` for configuration constants.

---

## Entry point

- **`main.py`**
  - Sets Streamlit page config and injects shared CSS via `frontend.ui.styles.inject_styles`.
  - Chooses data source:
    - When `backend.config.IS_AWS` is `True`, loads data from services using `_cached_aws_bootstrap`.
    - Otherwise, uses `backend.data_loader.load_data()` for a local/demo experience.
  - Initializes `st.session_state` with user/session defaults.
  - Handles deep-link query params for:
    - Opening a specific book detail by `book_id` or `source_id`.
    - Jumping directly to a forum post.
  - Renders the top-level tabs via `frontend.pages.tabs.render_tabs`, passing:
    - Books and events (clubs) and their lookup dicts.
    - User/session information and storage handles.
    - Cached helpers for recommendations and external services.

---

## Pages

The `pages` package holds view logic for each major area of the app:

- **`pages/auth.py`**
  - Authentication sidebar (`auth_panel`).
  - Account creation and genre-onboarding flows (`render_create_account_page`, `render_genre_onboarding`).
  - Integrates with `backend.services.auth_service` and `backend.services.library_service`.

- **`pages/feed.py`**
  - Main “Feed” tab and book detail page (`render_book_detail_page`).
  - Resolves recommended books (`resolve_recommended_books`) using:
    - `backend.services.recommender_service.get_recommended_books_for_user`.
    - Cached book recommendations from `main.py`.

- **`pages/explore_events.py`**
  - “Explore Events” tab for browsing book clubs and events.
  - Formats event time (`_format_when`) and works with:
    - `backend.services.events_service.get_explore_events`.
    - Saved-event helpers from `backend.services.user_events_service`.

- **`pages/my_events.py`**
  - “My Events” tab showing clubs/events relevant to the current user.
  - Synchronizes saved events/clubs with the backend (`_sync_user_clubs_and_save`).

- **`pages/library.py`**
  - “Library” tab for a user’s shelves (saved / in-progress / finished).
  - Uses `backend.services.library_service` for:
    - Adding/removing books.
    - Updating reading status.

- **`pages/forums.py`**
  - “Forum” tab listing posts and allowing navigation into discussions.
  - Provides formatting helpers:
    - Post/comment timestamps (`_format_post_time`, `_format_comment_time`).
    - Preview text (`_forum_preview_text`).
    - Tag construction and visibility checks (`build_post_tags`, `can_view_forum_post`).
  - Integrates with `backend.services.forum_service` and `backend.forum_store`.

- **`pages/tabs.py`**
  - Glue module that renders the five top-level tabs:
    - Feed, Explore Events, My Events, Library, Forum.
  - Delegates into the individual page modules.

---

## UI components and styles

The `ui` package contains shared UI helpers:

- **`ui/styles.py`**
  - Defines CSS and theme tweaks.
  - `inject_styles()` is called once from `main.py`.

- **`ui/components.py`**
  - Reusable Streamlit components for cards, lists, and layout primitives.
  - Used across the `pages` modules to keep layouts consistent.

---

## Data and recommendations

Although most of the recommendation logic lives in the backend, the frontend coordinates how it is displayed:

- Uses `backend.services.recommender_service.get_recommended_books_for_user` and `get_recommended_events_for_user` to display personalized content.
- Caches recommendations per user in `main.py` to avoid refetching on every interaction.
- Combines:
  - **Books** from services or `data_loader`.
  - **Events/clubs** from `events_service` and `user_events_service`.
  - **Forum posts** from `forum_service` and `forum_store`.

When developing new frontend features, prefer:

- Calling **services** from `backend.services.*`.
- Reusing helpers in `pages/*` and `ui/components.py`.
- Adding new configuration constants to `backend.config` instead of hard‑coding values in the UI.

