# Technology Review: scikit-learn vs TensorFlow

**Book Club Manager — Recommendation System**

---

## Overview

This technology review evaluates Python libraries for computing book-to-book cosine similarity as part of a personalized book recommendation system. The system combines Amazon book metadata, Amazon user reviews, and Seattle Public Library checkout data to surface relevant book recommendations for users. 

---

## The Problem

To power collaborative filtering in the recommender, we need to compute a **book-to-book cosine similarity matrix** from a user-book interaction matrix built from Amazon reviews. The raw dataset contains 571.54M rows. After filtering to positive reviews (rating ≥ 3) and constructing the interaction matrix, the result is extremely sparse — approximately 0.005% non-zero entries. The library we choose must handle this scale and sparsity efficiently.

---

## Libraries Evaluated

### scikit-learn

- **Author**: scikit-learn developers (INRIA, community maintained)
- **Summary**: General-purpose machine learning library for Python. Provides tools for classification, regression, clustering, dimensionality reduction, and preprocessing. Built on NumPy and SciPy, integrates natively with sparse matrices.
- **Relevant capability**: `sklearn.metrics.pairwise.cosine_similarity`, sparse matrix support via SciPy CSR format, `MinMaxScaler`, `TfidfVectorizer`

### TensorFlow

- **Author**: Google Brain Team
- **Summary**: Open-source deep learning framework optimized for large-scale numerical computation, particularly neural networks. Supports GPU acceleration and distributed training. High-level Keras API simplifies model building; lower-level API provides full flexibility.
- **Relevant capability**: `tf.linalg.matmul` for matrix multiplication, GPU acceleration, scalable to very large datasets

---

## Side-by-Side Comparison

| Criterion                     | scikit-learn                               | TensorFlow                                             |
| ----------------------------- | ------------------------------------------ | ------------------------------------------------------ |
| Sparse matrix support         | Native — operates directly on CSR matrices | No — requires conversion to dense tensor               |
| Memory (output matrix)        | Low — output stored as sparse              | High — dense float32 matrix                            |
| Ease of use                   | Simple API, minimal boilerplate            | More setup, especially for tabular data                |
| Integration with pandas/NumPy | Seamless                                   | Requires extra preprocessing                           |
| GPU acceleration              | No                                         | Yes                                                    |
| Best suited for               | Tabular, structured, sparse data           | Unstructured data, deep learning, large-scale training |
| Lines of code (this task)     | 6                                          | 8                                                      |
| One-time offline computation  | Well suited                                | Overkill                                               |

---

## Decision: scikit-learn

We chose `scikit-learn` for the following reasons:

1. **Sparse matrix support**: Our user-book interaction matrix is ~0.005% non-zero. scikit-learn operates natively on SciPy CSR sparse matrices. TensorFlow requires converting to a dense tensor, which would require substantially more memory for no benefit.

2. **Output is also sparse**: The resulting book similarity matrix is also sparse. scikit-learn produces and stores this as a sparse matrix (saved as `book_similarity.npz`), which is compact and fast to load at inference time.

3. **One-time computation**: Because our Amazon reviews dataset is static, the similarity matrix is computed once during preprocessing and saved to disk. Runtime speed is not a critical concern — correctness and memory efficiency are.

4. **Simpler API**: For this specific task (matrix normalization + matrix multiplication), scikit-learn requires fewer lines of code and no framework-specific setup.

5. **Sufficient for our ML tasks**: Beyond similarity computation, we use scikit-learn's `TfidfVectorizer` for genre feature extraction and `MinMaxScaler` for normalizing rating and popularity signals — both straightforward tabular operations that scikit-learn handles cleanly.

---

## Drawbacks

- **Scale ceiling**: scikit-learn is not optimized for GPU acceleration. If the dataset grows significantly beyond the current 571.54M rows, or if we move to a non-static dataset requiring frequent recomputation, TensorFlow's GPU support would provide a meaningful advantage.

- **Not suited for deep learning**: If future versions of the recommender incorporate neural collaborative filtering or embedding-based approaches (e.g., learning latent user/book representations), TensorFlow or PyTorch would be required. scikit-learn cannot support these architectures.

- **No distributed training**: scikit-learn runs on a single machine. For production-scale systems with continuous retraining, a distributed framework would be necessary.

---

## Demo

The demo script `technology_review_demo.py` in this folder shows the full comparison end-to-end.

### Setup

```bash
pip install scikit-learn tensorflow numpy scipy
```

### Run

```bash
python docs/technology_review/technology_review_demo.py
```

### What it does

1. Constructs a synthetic sparse user-book matrix (50,000 users × 10,000 books, 0.005% density) that mimics the real Amazon reviews matrix structure
2. Computes book-to-book cosine similarity using scikit-learn (sparse operations)
3. Computes the same similarity using TensorFlow (dense operations)
4. Prints a side-by-side comparison of time, memory usage, output sparsity, and top similar books for a sample book

### Expected output

```
==============================
TECHNOLOGY REVIEW DEMO: scikit-learn vs TensorFlow
Task: Book-to-book cosine similarity from user review matrix
==============================

Synthetic matrix: 50,000 users × 10,000 books
Density: 0.0050% non-zero entries

── scikit-learn ──────────────────────────────────────────
  Time          : 0.8s
  Output shape  : (10000, 10000)
  Non-zero pairs: ...
  Memory (sparse): ~X MB

── TensorFlow ────────────────────────────────────────────
  Time          : Xs
  Output shape  : (10000, 10000)
  Memory (dense): ~400 MB
  ...

COMPARISON SUMMARY
...
```

This technology review does not connect to the full original data due to size constraints, and the functionalities have been explored to arrive at a decision to proceed building a book recommender for the original data. 
