## Backend overview

This `backend` package contains the server-side logic for Book Club Manager. It is organized by responsibility:

- **Configuration (`config.py`)**: environment flags, paths, table names, tunables.
- **Storage (`storage.py`, `local_storage.py`, `cloud_storage.py`, `user_store.py`, `forum_store.py`)**: low-level data access for users, books, events, and forums.
- **Services (`services/*.py`)**: business logic that the UI or API should call.
- **Recommenders (`recommender/*.py`)**: book and event recommendation engines.
- **Utilities (`data_loader.py`, `service.py`, `api.py`)**: bootstrap data and simple facades.

The Streamlit app (and any future API) should call **services**, not storage or recommenders directly.

---

## Configuration

The `config.py` module is the single source of truth for:

- Environment mode (`APP_ENV=local|aws`, `IS_LOCAL`, `IS_AWS`).
- Local data directories (`PROCESSED_DIR`, `USERS_DIR`, JSON “databases” paths).
- DynamoDB table names, partition/sort keys, and GSI names.
- S3 bucket and key names for book data and static recommendation payloads.
- Tuning constants for recommenders (list sizes, recency pool sizes) and UI limits.

When you change infrastructure (tables, buckets, GSIs), update `config.py` instead of hard‑coding values in services.

---

## Storage layer

Storage modules are responsible for reading and writing data. They abstract over local JSON files and AWS (DynamoDB + S3).

- **`storage.py`**: main façade that exposes `get_storage()` and shared helpers.  
  In `local` mode it returns `LocalStorage`; in `aws` mode it returns `CloudStorage`.
- **`local_storage.py` / `cloud_storage.py`**: thin re‑exports of the concrete storage implementations.
- **`user_store.py`**: helpers to load/save all user‑related JSON stores and to project them into a merged user shape.
- **`forum_store.py`**: load/save forum posts and normalize missing fields.

To develop against AWS, set **`APP_ENV=aws`** so `get_storage()` returns `CloudStorage`. You can still run Streamlit locally while all data access goes through DynamoDB/S3.

---

## Service layer

Service modules define the operations the rest of the app should use. They encapsulate validation, normalization, and orchestration of storage and recommenders.

- **`services/auth_service.py`**: account creation and login, password hashing, bootstrap of per‑user data.
- **`services/books_service.py`**: book detail and discovery helpers (trending lists, book hub, related events/forums).
- **`services/events_service.py`**: read‑only operations for events (event detail, browse by city, “explore events” pool).
- **`services/library_service.py`**: user library and genre preferences (shelf changes, status updates, removing from shelves).
- **`services/forum_service.py`**: forum posts and threads (create posts/comments, likes, saved posts, filtering and sorting).
- **`services/user_events_service.py`**: per‑user saved events (get/add/remove/is_saved).
- **`services/recommender_service.py`**: high‑level book + event recommendation entry points, backing the homepage/feed.

Call these modules from the UI or API; they, in turn, call `get_storage()` and the recommender classes.

---

## Recommenders

The `recommender` package contains the algorithms and glue for recommendations:

- **`recommender/book_recommender.py`**: book recommendation model (and its training/usage helpers).
- **`recommender/event_recommender.py`**: scoring‑based event recommender (recency + genre/tag overlap + exploration).
- **`recommender/recommender_fitting.py`, `book_recomender_fitting.py`, `book_recommender_evaluation.py`**: offline training/evaluation utilities.
- **`recommender/config.py`**: recommender‑specific configuration.

Services should not import these directly except through `recommender_service.py`.

---

## Bootstrap and facades

Several modules provide thin facades or bootstrap data for the UI:

- **`data_loader.py`**: builds a mock “UI bootstrap” dict by reading processed JSONL/CSV files (used for the demo/seed experience).
- **`service.py`**: simple helper functions for building recommender inputs and returning book recommendations.
- **`api.py`**: placeholder for a future HTTP API (FastAPI or similar); currently imports nothing heavy and documents the intended shape.

These helpers are optional; the main contract is storage → services → UI/API.

---

## How the pieces fit together

- **Homepage / feed**
  - UI → `backend.services.recommender_service.get_recommended_books_for_user(...)`
  - UI → `backend.services.recommender_service.get_recommended_events_for_user(...)`
- **Book detail**
  - UI → `backend.services.books_service.get_book_hub(parent_asin)`
    (book detail + related events + forum thread).
- **Events / explore**
  - UI → `backend.services.events_service.get_explore_events(...)`
  - UI → `backend.services.events_service.get_events_by_city(...)`
- **User library**
  - UI → `backend.services.library_service.add_book_to_library(...)`, `update_book_status(...)`, `remove_book_from_shelf(...)`
  - Library changes call `on_book_added_to_shelf` in `recommender_service`, which in turn may refresh book recommendations.

In short:

- **Config** defines where and how data lives.
- **Storage** knows **how** to talk to files/AWS.
- **Services** know **what** to do for the product.
- **Recommenders** decide **which** books and events to suggest.

