# Recommender Module

This folder contains the recommendation logic used by the Bookish backend.

The system provides:
- Book club event recommendations (in progress)
- Book recommendations (in progress)

The recommender receives user preferences from the backend API and returns
a ranked list of results based on relevance.

---

## Structure

---


## Events Recommender (In Progress)

The events recommender suggests book club events using data from sources
such as SerpAPI and library datasets.

### Ranking Rules (current notebook logic)
- Keep only upcoming events.
- Normalize `tags` to lowercase; user inputs are `user_tags` plus optional `preferred_city`.
- Per-event scoring:
  - `tag_overlap` = shared tags; `tag_score = min(3, tag_overlap)`.
  - `recency_score`: strong boost for next ~14 days, tapers to ~0 by ~45 days, penalizes beyond.
  - `city_score`: +2 if `preferred_city` is in `city_state`.
  - `venue_score`: `0.2 * venue_rating` when numeric.
  - `score = 0.5 + 1.5*recency_score + 1.0*city_score + 0.75*tag_score + 0.2*venue_score`.
- Ordering: sort by `score` desc, then earlier `start_iso`; if the top item is tagged `trivia`, swap in the first non-trivia item when available.
- Exploration: every 3rd slot, try to inject a zero-overlap event; then backfill from the explore pool and the ranked list until `top_k`.

### Inputs
- User genres or keywords  
- User location (optional)  
- Event dataset with fields like title, description, time, and location  

### Outputs
A ranked list of events including title, time, location, and score.

Results are returned through the backend endpoint:

POST /recommendations/events

### Ranking Strategy
Events are ranked using a simple priority order:

1. Topic match + upcoming time  
2. Location match + time  
3. Location match + topic  

This approach favors events that are both relevant and practical to attend.

---

## Books Recommender (In Progress)

A book recommendation module is being developed separately. It will use
metadata and review datasets to suggest books tailored to user interests
and club activity.

Details will be added as this component is implemented.