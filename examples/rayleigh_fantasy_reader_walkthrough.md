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

## Package/Service calls that support this flow

- Event discovery and detail:
  - `backend.services.events_service.search_events()`
  - `backend.services.events_service.get_event_detail()`
  - `backend.services.events_service.get_upcoming_events()`
- Personalized recommendations:
  - `backend.services.recommender_service.recommend_all_for_user()`
  - `backend.services.recommender_service.recommend_books_for_user()`
- Book pages and library updates:
  - `backend.services.books_service.get_book_with_description()`
  - `backend.services.library_service.save_book_to_library()`
  - `backend.services.library_service.set_book_status()`
- Discussion/forum:
  - `backend.services.forum_service.get_thread_for_book()`
  - `backend.services.forum_service.add_post_to_thread()`
