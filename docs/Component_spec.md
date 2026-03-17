# Component Specifications

## Software Components

### Component 1: Book Recommender

**Description:**  
Generates personalized book recommendations based on user preferences and popularity signals.

**Inputs:**

- User preferred genres (optional)
- User reading history (optional)
- Book ratings and number of reviews
- SPL checkout data (when available)
- Book club/event associations (when available)

**Outputs:**

- Ranked list of recommended books

**Implementation:**

- `backend/recommender/book_recommender.py`
- `backend/services/recommender_service.py`

---

### Component 2: Book Club / Event Recommender

**Description:**  
Recommends book clubs and events based on user preferences and context.

**Inputs:**

- User preferred genres (optional)
- Location / neighborhood
- Event metadata (tags, genres, date/time, TTL)

**Outputs:**

- Ranked list of recommended book clubs/events

**Implementation:**

- `backend/recommender/event_recommender.py`
- `backend/services/recommender_service.py`

---

### Component 3: My Events / Book Club Pages

**Description:**  
Displays and manages book clubs and events a user has joined.

**Inputs:**

- User event memberships
- Event metadata (schedule, location, book)

**Outputs:**

- Centralized dashboard of joined events
- Access to individual book club pages and discussions

**Implementation:**

- `backend/services/user_events_service.py`
- `backend/storage.py`

---

### Component 4: Library

**Description:**  
Tracks user reading activity and preferences.

**Inputs:**

- Saved books
- Reading status (Saved / In Progress / Finished)
- User genre preferences

**Outputs:**

- Personal reading dashboard
- Input signals for recommendation engine

**Implementation:**

- `backend/services/library_service.py`
- `backend/local_storage.py`
- `backend/storage.py`

---

### Component 5: Forum

**Description:**  
Supports discussions for both public and private book communities.

**Inputs:**

- Public: title, genre (optional), book (optional)
- Private: book club, members, title, genre

**Outputs:**

- Discussion threads (global and book club-specific)

**Implementation:**

- `backend/forum_store.py`
- `backend/services/forum_service.py`
- `backend/storage.py` (DynamoDB / local)

---

### Component 6: Individual Book Page

**Description:**  
Displays detailed information about a specific book.

**Inputs:**

- Book metadata (Amazon dataset)
- SPL availability (when available)
- Related events/book clubs

**Outputs:**

- Book details view (description, ratings, availability)
- Option to save to Library

**Implementation:**

- `backend/data_loader.py`
- `backend/local_storage.py`
- `backend/storage.py`

---

## Component Interactions

### Use Case 1: Find and Join a Book Club/Event

1. User opens Bookish (optional login)
2. User enters preferences (genre, location, availability)
3. System retrieves events from:
   - Local processed data OR
   - Cloud storage (DynamoDB)
4. Event Recommender ranks results
5. User views event details
6. User joins event or follows external link
7. System updates “My Events”

````mermaid
flowchart LR
    A[User] --> B[Frontend UI]
    B --> C[Event Recommender Service]
    C --> D[Storage Layer]
    D --> E[Event Data]
    C --> B
    B --> F[Event Page]
    F --> G[User Events Service]
    G --> H[My Events]```

### Use Case 2: Book Recommendations and Library

1. User visits Feed
2. User provides preferences (optional)
3. System generates recommendations using:
   - Amazon metadata
   - SPL data (optional)
   - User activity
4. Recommendations are displayed as UI cards
5. User views book details
6. User saves book with a status (Saved / In Progress / Finished)
7. Library updates and improves future recommendations

```mermaid
flowchart LR
    A[User] --> B[Feed Page]
    B --> C[Recommender Service]
    C --> D[Book Recommender]
    D --> E[Data Sources]
    E --> F[Amazon + SPL Data]
    C --> B
    B --> G[Book Page]
    G --> H[Library Service]
    H --> I[User Library]```
````
