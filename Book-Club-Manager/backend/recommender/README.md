# Recommender Module

This folder contains the recommendation logic used by the Bookish backend.

The system provides:
- Book club event recommendations (implemented)
- Book recommendations (in progress)

The recommender receives user preferences from the backend API and returns
a ranked list of results based on relevance.

---

## Structure

---


## Events Recommender (In Progress)

The events recommender suggests book club events using data from sources
such as SerpAPI and library datasets.

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

## Books Recommender

Single implementation:

- **`book_recommender.py`** – Single book recommender: ML (logistic regression + similarity) when artifacts exist; otherwise returns **reviews_top50_books** from `get_storage().get_top50_review_books()`.

### How the app calls it

1. **`recommender_service.get_book_recommendations(user_id)`** builds `user_book_ids` from storage (library shelves), then calls `BookRecommender().recommend(user_book_ids, top_k)`. Cold start: pass `[]`.

2. **`BookRecommender()`** returns a cached ML instance or a fallback that uses `get_storage().get_top50_review_books()` and returns the first `top_k`.

3. **Example:** `example_use/example_users_recs.py` uses `BookRecommender()` from `backend.recommender.book_recommender` and calls `recommender.recommend(user_book_ids, top_k=50)`.

**Temporary (no ML artifacts):** Set `USE_BOOK_ML_RECOMMENDER=0` or leave unset to always use the fallback. Fallback reads **reviews_top50_books** from storage: local JSON when `APP_ENV=local`, S3 when `APP_ENV=aws`. Set `APP_ENV=aws` on EC2 to use cloud.