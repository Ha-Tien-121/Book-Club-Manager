# Example 2: Ben finds a UX/UI learning book club

## Persona

- Name: Ben
- Goal: join a professional business-focused book club that fits a busy schedule
- Constraints: wants fast discovery and high-signal recommendations

## Step-by-step walkthrough

1. Ben opens Bookish and create an account.
2. He enters profile genre preferences:
   - Genres: Business & Money, Science & Math
3. He goes to the Explore Events tab and filters events to "Business & Money"
4. Bookish shows matching events with time, location, and links.
5. He compares options and opens the event listing links for matches.
6. He joins one club that matches his time window.
7. In the Feed tab, he reviews his personalized recommendations. He filters for business-related reading.
8. He opens a recommended title and saves it to his library.
9. He sets the reading status to In Progress.
10. He visits the forum thread for that book and posts a question for peers.
11. He checks My Events to plan attendance for the next session.
12. After finishing the book, he marks it Finished to improve future recommendations.

## Expected outcome

- Ben finds and joins a relevant professional club quickly.
- He receives useful recommendations tied to his interests.
- His activity (save/status updates/discussion) feeds a stronger personalized experience over time.

## Package/Service calls that support this flow

- Club/event search and filtering:
  - `backend.services.events_service.search_events()`
  - `backend.services.events_service.get_events_by_tag()`
  - `backend.services.events_service.get_event_detail()`
- Recommendations:
  - `backend.services.recommender_service.recommend_books_for_user()`
  - `backend.services.recommender_service.recommend_all_for_user()`
- Book detail and user library:
  - `backend.services.books_service.get_book_with_description()`
  - `backend.services.library_service.save_book_to_library()`
  - `backend.services.library_service.set_book_status()`
  - `backend.services.library_service.acknowledge_recommendations_ran()`
- Forum participation:
  - `backend.services.forum_service.get_thread_for_book()`
  - `backend.services.forum_service.add_post_to_thread()`
