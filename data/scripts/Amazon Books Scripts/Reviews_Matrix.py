"""
Cleans the Amazon book reviews data and transforms it into a user-book sparse matrix (X)
then a sparse book similarity matrix (book_similarity_sparse).

Data cleaning steps:
(1) removes reviews with rating < 3 (not positive reviews)
(2) only keeps user_id and parent_asin (book id) columns
(3) drops duplicate reviews by same user for same book and rows with na values
Matrix construction steps:
(4) maps user_id and parent_asin to integer indices for sparse matrix construction
(5) constructs user-book sparse matrix X where X[u, b] = 1 if user u has reviewed book b
(6) calculates cosine similarity of columns (books) in X to get book similarity matrix

Args:
    Books.jsonl: The input dataset of Amazon book reviews in JSON Lines format.

Returns:
    X: A sparse user-book matrix where X[u, b] = 1 if user u has reviewed book b with rating >= 3.
    book_similarity_sparse: A sparse matrix where entry (i, j) is the cosine similarity of book i
    and book j based on user reviews.
"""
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.preprocessing import normalize


input_file = "Books.jsonl"

chunksize = 1000000
filtered_chunks = []

for chunk in pd.read_json(input_file, lines=True, chunksize=chunksize):

    chunk = chunk[['user_id', 'parent_asin', 'rating']]
    chunk = chunk[chunk['rating'] >= 3]
    chunk = chunk.dropna(subset=['user_id', 'parent_asin'])
    chunk = chunk[['user_id', 'parent_asin']]
    filtered_chunks.append(chunk)

df = pd.concat(filtered_chunks, ignore_index=True)
df = df.drop_duplicates(['user_id', 'parent_asin'])

df['user_idx'], user_index = pd.factorize(df['user_id'])
df['book_idx'], book_index = pd.factorize(df['parent_asin'])

n_users = df['user_idx'].nunique()
n_books = df['book_idx'].nunique()

rows = df['user_idx'].to_numpy()
cols = df['book_idx'].to_numpy()
values = np.ones(len(df), dtype=np.int8)

# Look at non-zero entries in each row to see which books each user has reviewed
X = csr_matrix((values, (rows, cols)), shape=(n_users, n_books), dtype=int)

### Calculate cosine similarity of columns (books) ###
# counts number of users who read books i and j normalized by number users who read i and number
# users who read j
X_normalized = normalize(X, axis=0)
book_similarity_sparse = X_normalized.T @ X_normalized 