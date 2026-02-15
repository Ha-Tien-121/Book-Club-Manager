import json
import csv
from collections import defaultdict

input_file = "Books.jsonl"
output_file = "Reviews_cleaned.csv"

user_books = defaultdict(set)
with open(input_file, 'r') as fp, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    for line in fp:
        review = json.loads(line)
        if review['rating'] >= 3:  # Only include reviews with rating 3 or higher
            user_id = review.get('user_id')
            parent_asin = review.get('parent_asin')
            
            if user_id and parent_asin:
                user_books[user_id].add(parent_asin)

with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(['user_id', 'books'])
    for user_id, books in user_books.items():
        if len(books) > 1:  # Only include users who reviewed more than one book
            writer.writerow([user_id, list(books)])