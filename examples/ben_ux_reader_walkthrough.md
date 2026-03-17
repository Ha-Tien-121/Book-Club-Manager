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

## Functional touchpoints and UI design rationale

- Club/event search and filtering:
  - `search_events()`, `get_events_by_tag()`, `get_event_detail()`
  - UI rationale: combining search + tag filters supports both "I know what I want" and "let me browse quickly" behaviors for busy users.

- Recommendations:
  - `recommend_books_for_user()`, `recommend_all_for_user()`
  - UI rationale: a dedicated "Recommended for you" section in Feed gives a place to check tailored picks each session.

- Book detail and library state updates:
  - `get_book_with_description()`, `save_book_to_library()`, `set_book_status()`, `acknowledge_recommendations_ran()`
  - UI rationale: placing the library status selector on the book detail page lets users act immediately after evaluating a book, reducing extra clicks.

- Forum participation:
  - `get_thread_for_book()`, `add_post_to_thread()`
  - UI rationale: forum access from book details page keeps peer discussion integrated with discovery and progress tracking, rather than separate.
