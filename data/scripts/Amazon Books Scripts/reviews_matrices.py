"""
Filters the Amazon book reviews data and splits it into train (75%) and test (25%) sets. 
Transforms data in each split into a user-book sparse matrix with a corresponding
vector of ground truth books indices (dense) for evaluation. Calculates the cosine similarity of
pairs of books based on the train user-book matrix.

*** Data filtering steps: ***
(1) does not include reviews with rating < 3 (not positive reviews)
(2) only stores user_id and parent_asin (book id) columns
(3) drops duplicate reviews by same user for same book and rows with na values
** Train-test split and cosine similarity matrix construction steps: ***
(4) maps user_id and parent_asin to integer indices for sparse matrix construction
(5) constructs user-book sparse matrices, TRAIN (75%) and TEST (25%), along with corresponding 
column vectors of ground truth book indices for evaluation, TRAIN_GROUND_TRUTH and 
TEST_GROUND_TRUTH.
(6) constructs sparse book similarity matrix of cosine books, BOOKS_SIMILARITY_SPARSE.

Args:
    Books.jsonl: The input dataset of Amazon book reviews.

Returns:
    train_matrix.npz : NPZ file containing scipy.sparse.csr_matrix TRAIN,
                       Dimension (total # users, total # books),
                       Entry (u, b) = 1 if user u in train user-book matrix has reviewed book b and
                       0 otherwise.
     test_matrix.npz : NPZ file containing scipy.sparse.csr_matrix TEST,
                       Dimension (total # users, total # books),
                       Entry (u, b) = 1 if user u in test user-book matrix has reviewed book b and 0
                       otherwise.
    book_similarity.npz : NPZ file containing scipy.sparse.csr_matrix BOOK_SIMILARITY_SPARSE,
                          Dimension (total # books, total # books),
                          Entry (i, j) is the cosine similarity of book i and book j based on train 
                          user-book matrix.
    train_ground_truth.npy : NPY file containing dense numpy.ndarray TRAIN_GROUND_TRUTH,
                             Dimension (total # users, 1),
                             Each row corresponds to a user and contains the index of the held-out 
                             book for that user in the train split (or -1 if no book was held out).
    test_ground_truth.npy : NPY file containing dense numpy.ndarray TEST_GROUND_TRUTH,
                            Dimension (total # users, 1),
                            Each row corresponds to a user and contains the index of the held-out 
                            book for that user (or -1 if no book was held out).

Note:
    - total # user and total # books are determined by the number of unique user_id and parent_asin
    in the filtered data.

Time: ~ 20 minutes to run
"""

import json
import random

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.sparse import save_npz


def create_leave_n_out_split(df, user_idx_col_name, book_idx_col_name,
                                selected_user_indices, ground_truth_set_size=1):
    """
    Construct a sparse user-book matrix and a corresponding ground truth vector from subset of data.

    This function performs a leave-n-out split per user. For each user, ground_truth_size books are
    held out and their indices are stored in a ground truth array, while the remaining interactions 
    are used to construct a CSR training matrix.

    Args:
        df : pandas.DataFrame
             Full data frame containing integer user idx and book idx for user-book interactions.
        user_idx_col_name : str
                            Column name containing integer user indices that we map to 
                            the rows of the CSR matrix.
        book_idx_col_name : str
                            Column name containing integer book indices that we map to the 
                            columns of the CSR matrix.
        selected_user_indices : list of int
                                List of user indices to include in the split. Only interactions for 
                                these users will be considered. 
        ground_truth_size : int, default=1
                            Number of interactions to hold out per user.

    Returns: 
        split_matrix : scipy.sparse.csr_matrix
                       Sparse matrix of shape (total # of users, total # of books) containing only 
                       the split interactions (not held-out). Each row corresponds to a user and 
                       each column corresponds to a book. An entry of 1 indicates that the user 
                       interacted with the book and an entry of 0 indicates no interaction.
        ground_truth : numpy.ndarray
                       Array of shape (total # of users, ground_truth_size) containing
                       held-out book indices for each user in split. Each row corresponds to a user 
                       and contains the indices of the held-out books for that user. If no book 
                       was held out for a user, the row contains -1.                       

    Notes:
        - If a user has fewer or equal interaction to the specified ground_truth_size or they are 
          not in selected_user_indices, they have value -1 for all entries in ground truth vector.
        - For the same df, the dimensions of split_matrix and ground_truth will be the same for any 
          set of valid selected_user_indices and ground_truth_size. Adjusting these parameters will 
          only change the number of nonzero entries.
        - *** IMPORTANT: the parameters user_idx_col_name and book_idx_col_name may need to be 
          dervied from the user_id and book_id using factorization or mapping to integer 
          indices. ***
    """
    n_users = df[user_idx_col_name].nunique()
    n_books = df[book_idx_col_name].nunique()
    ground_truth_col_vec = np.zeros((n_users, ground_truth_set_size), dtype=int) - 1

    split_df = df.loc[df[user_idx_col_name].isin(selected_user_indices)]
    rows = split_df[user_idx_col_name].to_numpy()
    cols = split_df[book_idx_col_name].to_numpy()
    values = np.ones(len(rows), dtype=np.int8)
    split_matrix = csr_matrix((values, (rows, cols)), shape=(n_users, n_books), dtype=int)
    for row_indx in selected_user_indices:
        if split_matrix[row_indx].nnz <= ground_truth_set_size:
            continue
        random_book = random.sample(list(split_matrix[row_indx].indices), k = ground_truth_set_size)
        ground_truth_col_vec[row_indx, :] = random_book
        split_matrix[row_indx, random_book] = 0
    split_matrix.eliminate_zeros()
    return split_matrix, ground_truth_col_vec


INPUT_FILE = "Books.jsonl"

filtered_books = []
with open(INPUT_FILE, 'r', encoding='utf-8') as fp:
    for line in fp:
        book = json.loads(line)
        if book.get('rating', 0) >= 3 and book.get('user_id') and book.get('parent_asin'):
            filtered_books.append((book['user_id'], book['parent_asin']))

reviews_df = pd.DataFrame(filtered_books, columns=['user_id', 'parent_asin'])
reviews_dfdf = reviews_df.drop_duplicates(['user_id', 'parent_asin'])

reviews_df['user_idx'], user_index = pd.factorize(reviews_df['user_id'])
reviews_df['book_idx'], book_index = pd.factorize(reviews_df['parent_asin'])
n_unique_users = reviews_df['user_idx'].nunique()
train_n_rows = round(n_unique_users * 0.75)
train_row_indices = random.sample(range(n_unique_users), train_n_rows)
print("Cleand data")

TRAIN, TRAIN_GROUND_TRUTH = create_leave_n_out_split(reviews_df, 'user_idx', 'book_idx',
                         train_row_indices, ground_truth_set_size=1)


test_row_indices = list(set(range(n_unique_users)) - set(train_row_indices))
TEST, TEST_GROUND_TRUTH = create_leave_n_out_split(reviews_df, 'user_idx', 'book_idx',
                         test_row_indices, ground_truth_set_size=1)
print("Built train and test user-book matrices and corresponding ground truth vectors.")

### Calculate cosine similarity of columns (books) ###
col_norms = np.sqrt(TRAIN.power(2).sum(axis=0)).A1
col_norms[col_norms == 0] = 1.0
TRAIN_normalized = TRAIN.multiply(1 / col_norms)
BOOK_SIMILARITY_SPARSE = TRAIN_normalized.T @ TRAIN_normalized

save_npz("train_matrix.npz", TRAIN)
save_npz("test_matrix.npz", TEST)
save_npz("book_similarity.npz", BOOK_SIMILARITY_SPARSE)
np.save("train_ground_truth.npy", TRAIN_GROUND_TRUTH)
np.save("test_ground_truth.npy", TEST_GROUND_TRUTH)
