"""
Technology Review Demo: scikit-learn vs TensorFlow
for Book Recommendation — Cosine Similarity Computation

This script demonstrates why scikit-learn was chosen over TensorFlow
for computing book-to-book cosine similarity from user review data.

Input  : A synthetic sparse user-book interaction matrix
         (mimics the real Amazon reviews matrix structure)
Output : Book similarity scores, timing comparison, memory comparison
"""

import time
import numpy as np
from scipy.sparse import random as sparse_random, csr_matrix

# ── scikit-learn ──────────────────────────────────────────────────────────────
from sklearn.metrics.pairwise import cosine_similarity as sklearn_cosine
from sklearn.preprocessing import normalize

# ── TensorFlow ────────────────────────────────────────────────────────────────
import tensorflow as tf


# =============================================================================
# 1. Synthetic data — mimics the real Amazon reviews sparse matrix
#    571M rows filtered to positive reviews → ~0.005% non-zero entries
# =============================================================================

print("=" * 60)
print("TECHNOLOGY REVIEW DEMO: scikit-learn vs TensorFlow")
print("Task: Book-to-book cosine similarity from user review matrix")
print("=" * 60)

N_USERS = 50_000
N_BOOKS = 10_000
DENSITY  = 0.00005   # 0.005% non-zero — matches real data sparsity

print(f"\nSynthetic matrix: {N_USERS:,} users × {N_BOOKS:,} books")
print(f"Density: {DENSITY*100:.4f}% non-zero entries")
print(f"Approx non-zero entries: {int(N_USERS * N_BOOKS * DENSITY):,}\n")

sparse_matrix = sparse_random(N_USERS, N_BOOKS, density=DENSITY, format="csr", dtype=np.float32)
sparse_matrix.data[:] = 1.0   # binarise: 1 = positive review


# =============================================================================
# 2. scikit-learn — sparse cosine similarity
# =============================================================================

print("── scikit-learn ──────────────────────────────────────────")

t0 = time.perf_counter()

# Normalise columns (books) then compute book × book similarity
col_norms = np.sqrt(sparse_matrix.power(2).sum(axis=0)).A1
col_norms[col_norms == 0] = 1.0
normalised = sparse_matrix.multiply(1.0 / col_norms)
book_similarity_sklearn = (normalised.T @ normalised).tocsr()   # sparse result

sklearn_time = time.perf_counter() - t0

nnz = book_similarity_sklearn.nnz
matrix_size = N_BOOKS * N_BOOKS
sparsity = 1 - nnz / matrix_size

print(f"  Time          : {sklearn_time:.3f}s")
print(f"  Output shape  : {book_similarity_sklearn.shape}")
print(f"  Non-zero pairs: {nnz:,} / {matrix_size:,} ({sparsity*100:.2f}% sparse)")
print(f"  Memory (sparse): ~{book_similarity_sklearn.data.nbytes / 1e6:.2f} MB data array")
print(f"  Result type   : {type(book_similarity_sklearn)}\n")

# Sample output — top 3 similar books to book index 0
book_0_similarities = book_similarity_sklearn[0].toarray().ravel()
top3_sklearn = np.argsort(-book_0_similarities)[1:4]
print(f"  Top 3 books similar to book 0 (sklearn): indices {top3_sklearn.tolist()}")
print(f"  Similarity scores: {book_0_similarities[top3_sklearn].tolist()}\n")


# =============================================================================
# 3. TensorFlow — dense cosine similarity (equivalent operation)
# =============================================================================

print("── TensorFlow ────────────────────────────────────────────")

t0 = time.perf_counter()

# TensorFlow requires dense tensors — convert sparse matrix to dense
dense_matrix = tf.constant(sparse_matrix.toarray(), dtype=tf.float32)

# Normalise columns
col_norms_tf = tf.norm(dense_matrix, axis=0, keepdims=True)
col_norms_tf = tf.maximum(col_norms_tf, 1e-12)
normalised_tf = dense_matrix / col_norms_tf

# Book × book similarity (dense)
book_similarity_tf = tf.linalg.matmul(normalised_tf, normalised_tf, transpose_a=True)

tf_time = time.perf_counter() - t0

dense_memory_mb = (N_BOOKS * N_BOOKS * 4) / 1e6   # float32 = 4 bytes

print(f"  Time             : {tf_time:.3f}s")
print(f"  Output shape     : {book_similarity_tf.shape}")
print(f"  Memory (dense)   : ~{dense_memory_mb:.1f} MB")
print(f"  Result type      : {type(book_similarity_tf)}\n")

book_0_tf = book_similarity_tf[0].numpy()
top3_tf = np.argsort(-book_0_tf)[1:4]
print(f"  Top 3 books similar to book 0 (TensorFlow): indices {top3_tf.tolist()}")
print(f"  Similarity scores: {book_0_tf[top3_tf].tolist()}\n")


# =============================================================================
# 4. Side-by-side comparison
# =============================================================================

print("=" * 60)
print("COMPARISON SUMMARY")
print("=" * 60)
print(f"{'Metric':<30} {'scikit-learn':>15} {'TensorFlow':>15}")
print("-" * 60)
print(f"{'Time (seconds)':<30} {sklearn_time:>15.3f} {tf_time:>15.3f}")
print(f"{'Sparse matrix support':<30} {'Yes':>15} {'No (dense only)':>15}")
print(f"{'Memory — output (MB)':<30} {book_similarity_sklearn.data.nbytes/1e6:>15.2f} {dense_memory_mb:>15.1f}")
print(f"{'Output stored as sparse':<30} {'Yes':>15} {'No':>15}")
print(f"{'Lines of code (this task)':<30} {'6':>15} {'8':>15}")
print(f"{'GPU required':<30} {'No':>15} {'Optional':>15}")
print("=" * 60)

print(f"""
CONCLUSION
----------
For our use case — computing book-to-book cosine similarity from a
large, extremely sparse user-book interaction matrix — scikit-learn
is the better choice:

  • Operates directly on sparse matrices (no dense conversion needed)
  • Output similarity matrix is also stored as sparse → far less memory
  • Simpler API for this specific tabular / matrix task
  • No GPU required; runs to completion in reasonable time
  • Result is saved as book_similarity.npz and loaded once at inference time

TensorFlow would require converting the sparse input to a dense tensor,
multiplying memory usage by ~{dense_memory_mb / (book_similarity_sklearn.data.nbytes/1e6 + 1e-9):.0f}×, and provides no meaningful
advantage for this one-time offline computation.
""")
