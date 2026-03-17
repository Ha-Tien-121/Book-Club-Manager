To generate model artifacts to run ML book recomender locally run the following scripts:

1. books_meta_data.py 
    input: meta_Books.jsonl
    outputs: books.db
2. book_ratings_vector.py
    input: books.db
    outputs: book_ratings.npz
3. reviews.py:
    input: Books.jsonl
    output: train_matrix.npz, test_matrix.npz, book_similarity.npz, train_ground_truth.npy,
    test_ground_truth.npy, book_id_to_idx.json
4. book_recommender_fitting.py
    input: train_matrix.npz, train_ground_truth.npy, book_similarity.npz, book_ratings.npz
    output: book_recommender_model.pkl, feature_scaler.pkl
5. book_recommender_evaluation.py (Optional)
    input: book_recommender_model.pkl, feature_scaler.pkl, test_matrix.npz, test_ground_truth.npy, book_similarity.npz,
    book_ratings.npz"
    outputs: None, model comparison to poularity heurist at top
6. book_reommender.py will now ML recommender, rather than fallback on Bookish website (you will not get missing artifact warnings)
