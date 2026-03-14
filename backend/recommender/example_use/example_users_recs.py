"""
This script demonstrates how to use the BookRecommender class to generate recommendations 
The script also measures the time taken for initialization and recommendation generation.

Usage:
    Run script from the project root using:
    python -m backend.recommender.example_use.example_users_recs
"""

import json
import time

from backend.recommender.recommender_backend import BookRecommender

start_time = time.time()
rec = BookRecommender()
end_time = time.time()
print(f"Recommender initialized in {end_time - start_time:.2f} seconds")

start_time = time.time()
test_users = {
    "user_1": ["0593105419", "1472154649", "B07K6THRJH"],
    "user_2": ["1984827618", "B08H2C71GW"],
    "user_3": ["0593540484", "1472154657", "0735219109"],
    "user_empty": []
}

results = {}

for user_id, books in test_users.items():
    rec.storage.get_user_books = lambda uid, b=books: b

    recs = rec.recommend(user_id)

    results[user_id] = recs
end_time = time.time()
with open("test_recommendations.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

print(f"Recommendation took {end_time - start_time:.4f} seconds")
