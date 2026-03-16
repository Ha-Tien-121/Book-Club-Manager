## Backend overview

This `backend` package is the server-side for the Book Club Manager. It is organized by responsibility:

- **Storage layer (`storage.py`)**: low-level data access helpers
- **Service layer (`*_service.py`)**: business logic used by the UI/API
- **Recommenders (`recommender/`)**: book and event recommendation engines

The Streamlit app (and any future API) should talk to **services**, not directly to storage.

---

## Storage layer

### `storage.py` (In Progress)

Single module for reading/writing data in external systems (DynamoDB, S3, parquet, etc.).

- **Books**
  - `get_book_details(parent_asin, local_dir=None, engine="pyarrow")`  
    Read a detailed book record (including description) from parquet shards on S3 or a local dir.
  - `get_book_metadata(parent_asin)`  
    Read a book metadata record (without description) from DynamoDB. Intended for cards, library, lists.

- **Events**
  - `get_event_details(event_id)`  
    Read a full event record from the `events` DynamoDB table.

- **Users / library / forums / catalogs**
  - `get_catalog(parent_asin)` (TODO)  
  - `get_user_accounts(user_id)` (TODO)  
  - `get_user_books(user_id)` (TODO)  
  - `get_user_clubs(user_id)` (TODO)  
  - `get_user_forums(user_id)` (TODO)  
  - `get_form_thread(parent_asin)` (TODO) – forum thread for a book.

- **Library action counters (for triggering recommendations)**
  - `increment_library_actions_since_recs(user_id, threshold=3, counter_attr="actions_since_recs")`  
    Atomic DynamoDB counter per user. Returns a small dict including `actions_since_recs` and `should_run_recommender`.
  - `reset_library_actions_since_recs(user_id, counter_attr="actions_since_recs")`  
    Reset the counter to 0 once recommendations have been refreshed.

- **(Optional) cached event recommendations**
  - `get_cached_event_recs(user_id)` (TODO)  
  - `put_cached_event_recs(user_id, payload)` (TODO)  
    Placeholders in case we later store per‑user event rec JSON with `generated_at` / `next_expiry`.

All environment‑specific configuration (table names, buckets) is handled here via env vars like `BOOKS_TABLE`, `EVENTS_TABLE`, `USER_LIBRARY_TABLE`, `DATA_BUCKET`.

### Develop with AWS

Set **`APP_ENV=aws`** so `get_storage()` returns **CloudStorage**. All services then use DynamoDB and S3 instead of local files. You can run Streamlit **locally** and still read/write AWS—only the data layer talks to AWS; the app process stays on your machine. Use this when local storage is incomplete or you want to develop against the real backend:

- **Book recs:** Fallback list from S3 (`reviews_top50_books.json`). Set `USE_BOOK_ML_RECOMMENDER=0` (default) until the lite ML model is ready.
- **Users, library, recommendations, events, forum:** CloudStorage implements these with the DynamoDB tables and GSIs in `config.py`. Ensure tables exist and partition keys match (e.g. `user_id` for user_books/user_accounts/user_events, `user_email` for user_recommendations/user_forums, `post_id` for forum_posts).

---

## Service layer

Service modules define the **operations the UI/API should call**. They encapsulate business rules and orchestrate storage/recommenders.

### `books_service.py` (In Progress)

Operations for book discovery and book detail pages.

- `get_trending_books(limit=50)` (TODO)  
  Top N popular books in Seattle (likely from SPL checkouts).
- `get_book_with_description(parent_asin)`  
  For the book detail page; wraps `storage.get_book_details`.
- `get_book_without_description(parent_asin)`  
  For cards / library / lists; wraps `storage.get_book_metadata`.
- `get_books_by_genre(genre, limit=50)` (TODO)  
  Genre/category browsing.

### `events_service.py` (In Progress)

Operations for book‑related events and clubs.

- `get_event_detail(event_id)`  
  For the event detail page; wraps `storage.get_event_details`.
- `get_trending_events(limit=10)` (TODO)  
  High‑level “featured / popular” events.
- `get_upcoming_events(limit=10)` (TODO)  
  Soonest events, sorted by start time.
- `get_events_by_tag(tag, limit=10)` (TODO)  
  Filter by tag (e.g. Romance, Fantasy).
- `get_events_by_book(book_title, limit=10)` (TODO)  
  Events that discuss a specific book.
- `search_events(query, limit=20)` (TODO)  
  Keyword search over titles / books / descriptions.

### `library_service.py` (In Progress)

Single entry point for **user library mutations**. Every change to a user’s library should go through here so we can track actions and hook into recommendations.

- `save_book_to_library(user_id, parent_asin)` (skeleton)  
  TODO: write to the library table, then calls `storage.increment_library_actions_since_recs`.
- `set_book_status(user_id, parent_asin, status)` (skeleton)  
  TODO: mark a book as saved / reading / read, then bump the action counter.
- `remove_book_from_library(user_id, parent_asin)` (skeleton)  
  TODO: remove a book from the library, then bump the action counter.
- `acknowledge_recommendations_ran(user_id)`  
  Resets the library action counter via `storage.reset_library_actions_since_recs`.

The typical pattern is:

1. UI calls a `library_service` mutation.
2. The mutation writes to storage (TODOs now).
3. The mutation bumps the action counter and returns metadata about whether the recommender should be refreshed.

### `forum_service.py`

Operations for book discussion threads and posts.

- `get_thread_for_book(parent_asin)` (skeleton)  
  TODO: load the thread for a given book (wraps a storage helper).
- `list_threads_for_user(user_id, limit=20)` (skeleton)  
  TODO: recent threads the user has participated in.
- `add_post_to_thread(parent_asin, user_id, content)` (skeleton)  
  TODO: add a new top‑level post in a book’s thread.
- `reply_to_post(thread_id, post_id, user_id, content)` (skeleton)  
  TODO: add a reply to an existing post.
- `toggle_hide_post(thread_id, post_id, hidden)` (skeleton)  
  TODO: soft‑hide/unhide posts for moderation.

### `auth_service.py` (planned) 

Currently empty; intended home for:

- `get_current_user(...)`
- sign‑in / sign‑out helpers
- token/session utilities (if needed)

---

## Recommenders (In Progress)

### `recommender/book_recommender.py`

Skeleton book recommender, currently:

- Embeds a sample of 50 books from `books_sample_100.json`.
- `recommend_for_user(user_email)`  
  Returns those 50 books (no personalization yet).

Later this can be replaced with a real model that uses user library, ratings, tags, etc.

### `recommender/event_recommender.py`

Skeleton event recommender, currently:

- Embeds 10 events from `bookclubs_seattle_clean.json`.
- `recommend_for_user(user_email)`  
  Returns those 10 events (no personalization yet).

### `recommender_service.py`

Facade that exposes recommendation operations to the rest of the backend.

- `recommend_books_for_user(user_email)`  
  Delegates to `book_recommender.recommend_for_user`.
- `recommend_events_for_user(user_email)`  
  Delegates to `event_recommender.recommend_for_user`.
- `recommend_all_for_user(user_email)`  
  Convenience helper returning both:
  - `"books"`: personalized (or placeholder) book recs
  - `"events"`: event recs

This is the module the homepage or “For You” page should call for personalized suggestions.

---

## How the pieces fit together

- **Homepage (personalized):**
  - Streamlit → `backend.services.recommender_service.recommend_all_for_user(user_email)`
  - That calls the book and event recommenders, which may in the future look at:
    - user library via `library_service` / `storage`
    - tags, genres, past behavior

- **Book detail page:**
  - Streamlit → `backend.services.books_service.get_book_with_description(parent_asin)`
  - For related content:
    - `backend.services.forum_service.get_thread_for_book(parent_asin)`
    - `backend.services.events_service.get_events_by_book(book_title, ...)`

- **Events page / Explore clubs:**
  - Streamlit → `backend.services.events_service.get_upcoming_events(...)` or `get_trending_events(...)`

- **User library page:**
  - Mutations go through `backend.services.library_service` (save/read/reading/remove).
  - Library changes bump an action counter in `storage`, which can be used to decide when to refresh recommendations.

In general:

- **Storage** knows *how* to talk to AWS / files.
- **Services** know *what* to do for the product.
- **Recommenders** know *which* items to suggest.

