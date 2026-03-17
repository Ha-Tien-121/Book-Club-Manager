# Example 1: Rayleigh finds a fantasy reading community

## Persona

- Name: Rayleigh
- Goal: find an active fantasy-focused book club that fits her schedule
- Constraints: wants a simple workflow and clear progress tracking

## Step-by-step walkthrough

1. Rayleigh opens Bookish and creates an account.
2. She goes to the Welcome/preference input view and enters:
   - Genre: Fantasy
3. Bookish returns popular events in seattle. She filters the events near her
location and in the "Fantasy" genre. 
4. She opens a promising club and reviews:
   - Description
   - Date/time
   - Location
   - Event listing link
5. She clicks Join.
6. The club appears in her "My Clubs" area.
7. She switches to recommendations and filters for Fantasy books.
8. She opens a recommended book detail page and clicks Save.
9. She sets status to In Progress.
10. During the week, she posts in the book discussion thread.
11. After finishing the book, she marks it Finished to improve future recommendations.


## Expected outcome

- Rayleigh joins at least one relevant club.
- Her library and reading status are updated.
- She can participate in discussion and track upcoming meetings in one app.

## Functional touchpoints and UI design rationale

- Event discovery and filtering:
  - `search_events()`, `get_event_detail()`, `get_upcoming_events()`
  - UI rationale: the Explore Events view combines location and genre filters so users can narrow results in-place instead of jumping across pages.

- Personalized recommendations:
  - `recommend_all_for_user()`, `recommend_books_for_user()`
  - UI rationale: recommendations are in feed to keep discovery in the first page.

- Library as progress state:
  - `save_book_to_library()`, `set_book_status()`
  - UI rationale: a single status control (Saved -> In Progress -> Finished) makes tracking easy and makes progress explicit at a glance.

- Discussion attached to reading context:
  - `get_thread_for_book()`, `add_post_to_thread()`
  - UI rationale: placing discussion entry points on the book detail page reduces page switching and keeps conversation tied to the current title.
