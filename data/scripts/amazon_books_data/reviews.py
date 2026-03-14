"""
Processes the Amazon book reviews data and splits it into train (75% of users) and test (25% of 
users) sets. Transforms data in each split into a user-book sparse matrix with a corresponding
vector of ground truth books indices (dense) for evaluation. Calculates the cosine similarity of
pairs of books based on the train user-book matrix.

*** Data processing steps: ***
(1) maps all parent_asins of books in books_by_parent_asin.jsonl (cleaned amazon books meta data) 
to integer book indices
(2) creates a dataframe of reviews with valid parent_asin, user_id, and rating >= 3 and maps all
parent_asins to book indices (as defined  in step 1), dropping rows with parent_asin not in mapping
** Train-test split and cosine similarity matrix construction steps: ***
(4) constructs user-book sparse matrices, TRAIN (75%) and TEST (25%), along with corresponding 
column vectors of ground truth book indices for evaluation, TRAIN_GROUND_TRUTH and 
TEST_GROUND_TRUTH.
(5) constructs sparse cosine similarity matrix of TF-IDF normalized book interactions, 
BOOKS_SIMILARITY_SPARSE.

Args:
    Books.jsonl: The input dataset of Amazon book reviews.
    meta_Books.jsonl: The input dataset of Amazon books metadata in JSON Lines format, 
    which is used to map parent_asins to book indices to ensure consistency across datasets.

Returns:
    train_matrix.npz : NPZ file containing scipy.sparse.csr_matrix TRAIN,
                       Dimension (total # users, total # books),
                       Entry (u, b) = 1 if user u in train user-book matrix has reviewed book b 
                       with rating >= 3 and 0 otherwise.
     test_matrix.npz : NPZ file containing scipy.sparse.csr_matrix TEST,
                       Dimension (total # users, total # books),
                       Entry (u, b) = 1 if user u in test user-book matrix has reviewed book b 
                       with rating >= 3 and 0 otherwise.
    book_similarity.npz : NPZ file containing scipy.sparse.csr_matrix BOOK_SIMILARITY_SPARSE,
                          Dimension (total # books, total # books),
                          Entry (i, j) is the cosine similarity of book i and book j based on 
                          train user-book matrix and normalized by TF-IDF.
    train_ground_truth.npy : NPY file containing dense numpy.ndarray TRAIN_GROUND_TRUTH,
                             Dimension (total # users, 1),
                             Each row corresponds to a user and contains the index of the held-out 
                             book for that user in the train split (or -1 if no book was held out).
    test_ground_truth.npy : NPY file containing dense numpy.ndarray TEST_GROUND_TRUTH,
                            Dimension (total # users, 1),
                            Each row corresponds to a user and contains the index of the held-out 
                            book for that user (or -1 if no book was held out).

Note: Total # of books is determined by the number of books in the cleaned amazon 
books meta data, meta_Books.jsonl 

Usage:
    Run script from the project root using:
    python -m data.scripts.amazon_books_data.reviews

Time: ~ 30 minutes to run
"""

import json
import os
import random

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, save_npz, diags

from data.scripts.config import RAW_DIR, PROCESSED_DIR

random.seed(42)
def create_leave_n_out_split(candidate_df, user_idx_col_name,
                             book_idx_col_name, split_proportion,
                             total_n_books=None, total_n_users=None, ground_truth_set_size=1):
    """
    Construct a sparse user-book matrix and a corresponding ground truth vector from subset of data.

    This function performs a leave-n-out split per user. For each user, ground_truth_set_size books 
    are held out and their indices are stored in a ground truth array, while the remaining 
    interactions are recorded in a CSR training matrix.

    Args:
        candidate_df : pandas.DataFrame
             Data frame containing user reviews that are candidates for split. Must have columns
             for user idx (integer), book idx (integer), and rating (float).
        total_n_books : int
                        Total number of books (columns) in the output sparse matrix. This should be
                        consistent across train and test splits and should be determined by the 
                        number of unique books you are interested in.
        total_n_users : int
                        Total number of users (rows) in the output sparse matrix. This should be
                        consistent across train and test splits and should be determined by the 
                        number of unique users you are interested in.
        user_idx_col_name : str
                            Column name containing integer user indices that we map to 
                            the rows of the CSR matrix.
        book_idx_col_name : str
                            Column name containing integer book indices that we map to the 
                            columns of the CSR matrix.
        split_proportion : float in (0, 1)
                           Proportion of users to include in the split.
        ground_truth_set_size : int, default=1
                                Number of interactions to hold out per user.

    Returns: 
        split_matrix : scipy.sparse.csr_matrix
                       Sparse matrix of shape (total_n_users by total_n_books) containing only 
                       the split interactions (not held-out). Each row corresponds to a user and 
                       each column corresponds to a book. An entry of 1 indicates user and book 
                       interaction in split (not held out) and an entry of 0 otherwise.
        ground_truth : numpy.ndarray
                       Array of shape (total_n_users, ground_truth_set_size) containing
                       held-out book indices for each user. Each row corresponds to a user 
                       and contains the indices of the held-out books for that user. If no book 
                       was held out for a user, the row contains -1.
        split_compliment : pandas.DataFrame
                          Data frame containing the users that were not included in the split 
                          matrix. This is used to construct the test split after creating the 
                          train split.     

    Notes:
        - If a user has fewer or equal number of reviews in df to the specified 
          ground_truth_size or is not in split, they have value -1 for all entries in ground 
          truth vector.
        - If total_n_books or total_n_users is not provided, it will be set to the number of unique
          book or user indices in the input dataframe, df.
        - *** IMPORTANT: the parameters user_idx_col_name and book_idx_col_name may need to be 
          dervied from the user_id and book_id using factorization or mapping to integer 
          indices. book_id should be derived from all books, not just those in reviews.
          similarly user_id should be derived from all users. ***
    """
    if total_n_books is None:
        total_n_books = candidate_df[book_idx_col_name].nunique()
    if total_n_users is None:
        total_n_users = candidate_df[user_idx_col_name].nunique()

    unique_users = candidate_df[user_idx_col_name].unique()
    n_users_in_split = round(len(unique_users)* split_proportion)
    user_indices_in_split = random.sample(list(unique_users), n_users_in_split)
    users_in_split = candidate_df[candidate_df[user_idx_col_name].isin(user_indices_in_split)]

    ground_truth_col_vec = np.full((total_n_users, ground_truth_set_size), -1, dtype=int)
    held_out = set()
    for user, group in users_in_split.groupby(user_idx_col_name):
        books = group[book_idx_col_name].tolist()
        if len(books) <= ground_truth_set_size:
            continue
        sampled = random.sample(books, ground_truth_set_size)
        ground_truth_col_vec[user] = sampled
        held_out.update((user, sample) for sample in sampled)

    mask = [(row.user_idx, row.book_idx) not in held_out
         for row in users_in_split.itertuples()]
    users_in_split = users_in_split.loc[mask]

    rows = users_in_split[user_idx_col_name].to_numpy()
    cols = users_in_split[book_idx_col_name].to_numpy()
    values = np.ones(len(rows), dtype=int)
    split_matrix = csr_matrix((values, (rows, cols)), shape=(total_n_users, total_n_books),
                              dtype=int)

    split_compliment = candidate_df.drop(users_in_split.index)
    return split_matrix, ground_truth_col_vec, split_compliment

INPUT_FILE = os.path.join(RAW_DIR, 'Books.jsonl')

OUTPUT_FILE_TRAIN_MATRIX = os.path.join(PROCESSED_DIR, 'train_matrix.npz')
OUTPUT_FILE_TEST_MATRIX = os.path.join(PROCESSED_DIR, 'test_matrix.npz')
OUTPUT_FILE_BOOK_SIMILARITY = os.path.join(PROCESSED_DIR, 'book_similarity.npz')
OUTPUT_FILE_TRAIN_GROUND_TRUTH = os.path.join(PROCESSED_DIR, 'train_ground_truth.npy')
OUTPUT_FILE_TEST_GROUND_TRUTH = os.path.join(PROCESSED_DIR, 'test_ground_truth.npy')
BOOK_ID_TO_IDX = os.path.join(PROCESSED_DIR, "book_id_to_idx.json")

def main(input_file=INPUT_FILE, book_id_to_idx = BOOK_ID_TO_IDX,
         output_file_train_matrix=OUTPUT_FILE_TRAIN_MATRIX,
         output_file_test_matrix=OUTPUT_FILE_TEST_MATRIX,
         output_file_book_similarity=OUTPUT_FILE_BOOK_SIMILARITY,
         output_file_train_ground_truth=OUTPUT_FILE_TRAIN_GROUND_TRUTH,
         output_file_test_ground_truth=OUTPUT_FILE_TEST_GROUND_TRUTH):  # pylint: disable=too-many-arguments, too-many-positional-arguments
    """
    Process Amazon book reviews into train/test matrices and compute book similarity.
    Outputs are saved as NPZ/NPY files.
    """
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if not os.path.exists(book_id_to_idx):
        raise FileNotFoundError(f"Book index mapping file not found: {book_id_to_idx}")

    with open(book_id_to_idx, "r", encoding="utf-8") as f:
        book_id_to_idx = json.load(f)
    n_books = len(book_id_to_idx)

    chunks_list = []
    user_id_to_idx = {}
    next_user_idx = 0
    for chunk in pd.read_json(
        input_file,
        lines=True,
        dtype={"user_id": "string", "parent_asin": "string", "rating": "float"},
        chunksize=1_000_000):

        chunk = chunk[["user_id", "parent_asin", "rating"]]
        chunk = chunk[chunk["rating"] >= 3]
        chunk["book_idx"] = chunk["parent_asin"].map(book_id_to_idx)
        chunk = chunk.dropna(subset=["book_idx"])
        chunk["book_idx"] = chunk["book_idx"].astype(int)
        new_users = chunk["user_id"].unique()

        for u in new_users:
            if u not in user_id_to_idx:
                user_id_to_idx[u] = next_user_idx
                next_user_idx += 1
        chunk["user_idx"] = chunk["user_id"].map(user_id_to_idx)
        chunks_list.append(chunk[["user_idx","book_idx"]])
    reviews_df = pd.concat(chunks_list, ignore_index=True)
    n_users = reviews_df['user_idx'].nunique()

    train, train_ground_truth, compliment_train = create_leave_n_out_split(
        candidate_df=reviews_df,
        user_idx_col_name='user_idx',
        book_idx_col_name='book_idx',
        split_proportion=0.75,
        total_n_books=n_books)
    test, test_ground_truth, __ = create_leave_n_out_split(
        candidate_df=compliment_train,
        user_idx_col_name='user_idx',
        book_idx_col_name='book_idx',
        split_proportion=1,
        total_n_books=n_books,
        total_n_users=n_users)
    print("Built train and test user-book matrices and corresponding ground truth vectors.")

    ### Calculate cosine similarity of columns (books) ###
    idf = np.log((n_users + 1) / (train.getnnz(axis=0) + 1)) + 1
    train_tfidf = train @ diags(idf)
    col_norms = np.sqrt(train_tfidf.power(2).sum(axis=0)).A1
    col_norms[col_norms == 0] = 1.0
    train_normalized = train_tfidf.multiply(1 / col_norms)
    book_similarity_sparse = train_normalized.T @ train_normalized
    book_similarity_sparse.data = book_similarity_sparse.data.astype("float32")
    book_similarity_sparse.data = np.round(book_similarity_sparse.data, 2)
    book_similarity_sparse = book_similarity_sparse.tocsr()
    book_similarity_sparse.setdiag(0)
    book_similarity_sparse.eliminate_zeros()

    save_npz(output_file_train_matrix, train)
    save_npz(output_file_test_matrix, test)
    save_npz(output_file_book_similarity, book_similarity_sparse)
    np.save(output_file_train_ground_truth, train_ground_truth)
    np.save(output_file_test_ground_truth, test_ground_truth)

if __name__ == "__main__":
    main()
