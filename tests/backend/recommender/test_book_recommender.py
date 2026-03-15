import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.feature_extraction.text import TfidfVectorizer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.recommender.book_recommender import BookRecommender, GENRE_VOCAB


@pytest.fixture()
def fitted_recommender() -> BookRecommender:
    br = BookRecommender(data_dir=None)

    parent_asins = [f"A{str(i).zfill(3)}" for i in range(1, 11)]
    titles = [f"Book {i}" for i in range(1, 11)]
    authors = [f"Author {i}" for i in range(1, 11)]
    images = [None, "img2", None, "img4", None, "img6", None, "img8", None, "img10"]

    # Ensure only a subset of books belong to "Fantasy" so preferences move rankings.
    categories_list = [
        ["History"],  # A001
        ["Romance"],  # A002
        ["Fantasy"],  # A003
        ["Science Fiction"],  # A004
        ["Fantasy"],  # A005
        ["Business & Money"],  # A006
        ["Mystery, Thriller & Suspense"],  # A007
        ["Fantasy", "Action & Adventure"],  # A008
        ["Poetry"],  # A009
        ["Classics"],  # A010
    ]
    genre_text = ["|".join(genres) for genres in categories_list]

    average_rating = np.linspace(1.0, 5.0, 10)
    rating_number = np.array([5, 15, 30, 45, 60, 75, 90, 105, 120, 135], dtype=int)

    # Deterministic popularity normalization via simple min-max.
    avg_norm = (average_rating - average_rating.min()) / (average_rating.max() - average_rating.min())
    rn = rating_number.astype(float)
    rn_norm = (rn - rn.min()) / (rn.max() - rn.min())

    books_df = pd.DataFrame(
        {
            "parent_asin": parent_asins,
            "title": titles,
            "author_name": authors,
            "average_rating": average_rating,
            "rating_number": rating_number,
            "images": images,
            "categories_list": categories_list,
            "genre_text": genre_text,
            "average_rating_norm": avg_norm,
            "rating_number_norm": rn_norm,
        }
    )

    vectorizer = TfidfVectorizer(
        vocabulary=GENRE_VOCAB,
        tokenizer=lambda s: s.split("|") if s else [],
        token_pattern=None,
        lowercase=False,
        norm="l2",
    )
    book_tfidf = vectorizer.fit_transform(books_df["genre_text"].fillna("").astype(str))

    br.books_df = books_df
    br.tfidf_vectorizer = vectorizer
    br.book_tfidf = book_tfidf
    br.book_id_to_idx = {asin: i for i, asin in enumerate(parent_asins)}

    return br


def test_cold_start_returns_top_k(fitted_recommender: BookRecommender) -> None:
    top_k = 5
    recs = fitted_recommender.recommend("u1", user_genres_df=None, user_books_df=None, top_k=top_k)
    assert len(recs) == top_k
    required = {
        "book_id",
        "parent_asin",
        "title",
        "author_name",
        "average_rating",
        "rating_number",
        "images",
        "categories",
        "score",
    }
    for r in recs:
        assert required.issubset(r.keys())


def test_cold_start_scores_use_popularity_only(fitted_recommender: BookRecommender) -> None:
    recs = fitted_recommender.recommend("u1", user_genres_df=None, user_books_df=None, top_k=3)
    top = recs[0]
    row = fitted_recommender.books_df[fitted_recommender.books_df["parent_asin"] == top["parent_asin"]].iloc[0]
    expected = 0.7 * float(row["average_rating_norm"]) + 0.3 * float(row["rating_number_norm"])
    assert float(top["score"]) == pytest.approx(expected, rel=1e-12, abs=1e-12)


def test_genre_preferences_change_results(fitted_recommender: BookRecommender) -> None:
    cold = fitted_recommender.recommend("u2", user_genres_df=None, user_books_df=None, top_k=6)
    user_genres_df = pd.DataFrame(
        {"user_id": ["u2"], "genre": ["Fantasy"], "rank": [1]}
    )
    warm = fitted_recommender.recommend("u2", user_genres_df=user_genres_df, user_books_df=None, top_k=6)
    assert [r["parent_asin"] for r in cold] != [r["parent_asin"] for r in warm]


def test_read_books_excluded(fitted_recommender: BookRecommender) -> None:
    read = ["A003", "A010"]
    user_books_df = pd.DataFrame({"user_id": ["u3", "u3"], "parent_asin": read})
    recs = fitted_recommender.recommend("u3", user_genres_df=None, user_books_df=user_books_df, top_k=10)
    returned = {r["parent_asin"] for r in recs}
    assert "A003" not in returned
    assert "A010" not in returned


def test_read_books_influence_profile(fitted_recommender: BookRecommender) -> None:
    user_books_df = pd.DataFrame({"user_id": ["u4", "u4"], "parent_asin": ["A003", "A005"]})
    recs = fitted_recommender.recommend("u4", user_genres_df=None, user_books_df=user_books_df, top_k=5)
    assert len(recs) == 5
    returned = {r["parent_asin"] for r in recs}
    assert "A003" not in returned
    assert "A005" not in returned


def test_top_k_respected(fitted_recommender: BookRecommender) -> None:
    recs3 = fitted_recommender.recommend("u5", user_genres_df=None, user_books_df=None, top_k=3)
    recs7 = fitted_recommender.recommend("u5", user_genres_df=None, user_books_df=None, top_k=7)
    assert len(recs3) == 3
    assert len(recs7) == 7


def test_output_schema(fitted_recommender: BookRecommender) -> None:
    recs = fitted_recommender.recommend("u6", user_genres_df=None, user_books_df=None, top_k=8)
    for r in recs:
        assert isinstance(r["book_id"], str)
        assert isinstance(r["parent_asin"], str)
        assert r["book_id"] == r["parent_asin"]
        assert isinstance(r["title"], str)
        assert isinstance(r["author_name"], (str, type(None)))
        assert isinstance(r["average_rating"], float)
        assert isinstance(r["rating_number"], int)
        assert isinstance(r["images"], (str, type(None)))
        assert isinstance(r["categories"], list)
        assert isinstance(r["score"], float)


def test_scores_descending(fitted_recommender: BookRecommender) -> None:
    recs = fitted_recommender.recommend("u7", user_genres_df=None, user_books_df=None, top_k=10)
    scores = [r["score"] for r in recs]
    assert scores == sorted(scores, reverse=True)


def test_genre_rank_weighting(fitted_recommender: BookRecommender) -> None:
    # Include a second genre so normalization doesn't cancel the rank effect.
    df_rank1 = pd.DataFrame(
        {
            "user_id": ["u8", "u8"],
            "genre": ["Fantasy", "Romance"],
            "rank": [1, 2],
        }
    )
    df_rank3 = pd.DataFrame(
        {
            "user_id": ["u8", "u8"],
            "genre": ["Fantasy", "Romance"],
            "rank": [3, 2],
        }
    )

    v1 = fitted_recommender.build_user_profile("u8", df_rank1, pd.DataFrame())
    v3 = fitted_recommender.build_user_profile("u8", df_rank3, pd.DataFrame())
    fantasy_idx = GENRE_VOCAB.index("Fantasy")
    assert v1[fantasy_idx] > v3[fantasy_idx]


def test_cold_start_detection(fitted_recommender: BookRecommender) -> None:
    br = fitted_recommender
    assert br._is_cold_start("u9", None, None) is True
    assert br._is_cold_start("u9", pd.DataFrame(), pd.DataFrame()) is True

    genres_other_user = pd.DataFrame({"user_id": ["someone_else"], "genre": ["Fantasy"], "rank": [1]})
    books_other_user = pd.DataFrame({"user_id": ["someone_else"], "parent_asin": ["A001"]})
    assert br._is_cold_start("u9", genres_other_user, pd.DataFrame()) is True
    assert br._is_cold_start("u9", pd.DataFrame(), books_other_user) is True

    genres_this_user = pd.DataFrame({"user_id": ["u9"], "genre": ["Fantasy"], "rank": [1]})
    assert br._is_cold_start("u9", genres_this_user, pd.DataFrame()) is False

    books_this_user = pd.DataFrame({"user_id": ["u9"], "parent_asin": ["A001"]})
    assert br._is_cold_start("u9", pd.DataFrame(), books_this_user) is False

