## Amazon Books Data Pipeline

This pipeline processes Amazon Books data in **three sequential steps**:  
(1) clean metadata → (2) process reviews → (3) extract rating features.

---

## Execution Order

Run the scripts in the following order:

```bash
python -m data.scripts.amazon_books_data.books_meta_data
python -m data.scripts.amazon_books_data.reviews
python -m data.scripts.amazon_books_data.book_ratings_vectors
1. Clean Book Metadata

Script

python -m data.scripts.amazon_books_data.books_meta_data

Input

Books.jsonl — raw Amazon books metadata

Outputs

books.db — SQLite database of cleaned book metadata

book_id_to_idx.json — mapping from parent_asin → integer index

Description

Extracts author names and cleans formatting

Keeps only large image URLs

Filters to popular categories (genres)

Creates normalized title|author keys for fast lookup

2. Process Reviews & Build Matrices

Script

python -m data.scripts.amazon_books_data.reviews

Inputs

Books.jsonl — raw Amazon reviews dataset

meta_Books.jsonl — metadata (for consistent book indexing)

Outputs

train_matrix.npz — sparse user–book matrix (train)

test_matrix.npz — sparse user–book matrix (test)

book_similarity.npz — sparse cosine similarity matrix between books

train_ground_truth.npy — held-out train labels

test_ground_truth.npy — held-out test labels

Description

Filters reviews (rating >= 3)

Maps books to indices using metadata

Splits users into 75% train / 25% test

Builds sparse interaction matrices

Computes TF-IDF–normalized cosine similarity between books

3. Generate Book Rating Vectors

Script

python -m data.scripts.amazon_books_data.book_ratings_vectors

Input

books.db — cleaned metadata database

Output

book_ratings.npz

ratings_avg — average rating per book

rating_counts — log number of ratings per book

Description

Extracts rating statistics from the database

Stores them in compact NumPy format for efficient use in the recommender
