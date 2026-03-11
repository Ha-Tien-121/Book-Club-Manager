"""Forum posts persistence store."""

# from __future__ import annotations  # legacy duplicated section (kept inert)

import json

from app.config import FORUM_DB_PATH, PROCESSED_DIR


def load_forum_store(seed_posts: list[dict]) -> dict:
    """Load persisted forum store, seeding from defaults on first run."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    if not FORUM_DB_PATH.exists():
        initial_posts = []
        for idx, post in enumerate(seed_posts, start=1):
            initial_posts.append(
                {
                    "id": idx,
                    "title": post["title"],
                    "author": post["author"],
                    "genre": post.get("genre"),
                    "club": post.get("club"),
                    "club_id": None,
                    "book_id": None,
                    "book_title": None,
                    "tags": [],
                    "visibility": "club" if post.get("club") else "public",
                    "replies": post.get("replies", 0),
                    "likes": post.get("likes", 0),
                    "liked_by": [],
                    "time_ago": post.get("time_ago", "recently"),
                    "preview": post["preview"],
                    "comments": [],
                }
            )
        store = {"next_post_id": len(initial_posts) + 1, "posts": initial_posts}
        FORUM_DB_PATH.write_text(json.dumps(store, indent=2), encoding="utf-8")
        return store

    with FORUM_DB_PATH.open("r", encoding="utf-8") as file_obj:
        try:
            store = json.load(file_obj)
        except json.JSONDecodeError:
            store = {"next_post_id": 1, "posts": []}
    if "posts" not in store or not isinstance(store["posts"], list):
        store["posts"] = []
    if "next_post_id" not in store or not isinstance(store["next_post_id"], int):
        store["next_post_id"] = len(store["posts"]) + 1
    for post in store["posts"]:
        post.setdefault("liked_by", [])
        post.setdefault("comments", [])
        post.setdefault("visibility", "public")
        post.setdefault("club", None)
        post.setdefault("club_id", None)
        post.setdefault("genre", None)
        post.setdefault("book_id", None)
        post.setdefault("book_title", None)
        post.setdefault("tags", [])
        for comment in post["comments"]:
            comment.setdefault("liked_by", [])
            comment.setdefault("likes", 0)
    return store


def save_forum_store(store: dict) -> None:
    """Persist forum posts/comments store to disk."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    FORUM_DB_PATH.write_text(json.dumps(store, indent=2), encoding="utf-8")

"""Forum persistence and normalization."""

# from __future__ import annotations  # legacy duplicated section (kept inert)

import json

from app.config import FORUM_DB_PATH, PROCESSED_DIR


def load_forum_store(seed_posts: list[dict]) -> dict:
    """Load persisted forum store, seeding from defaults on first run."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    if not FORUM_DB_PATH.exists():
        initial_posts = []
        for idx, post in enumerate(seed_posts, start=1):
            initial_posts.append(
                {
                    "id": idx,
                    "title": post["title"],
                    "author": post["author"],
                    "genre": post.get("genre"),
                    "club": post.get("club"),
                    "club_id": None,
                    "book_id": None,
                    "book_title": None,
                    "tags": [],
                    "visibility": "club" if post.get("club") else "public",
                    "replies": post.get("replies", 0),
                    "likes": post.get("likes", 0),
                    "liked_by": [],
                    "time_ago": post.get("time_ago", "recently"),
                    "preview": post["preview"],
                    "comments": [],
                }
            )
        store = {"next_post_id": len(initial_posts) + 1, "posts": initial_posts}
        FORUM_DB_PATH.write_text(json.dumps(store, indent=2), encoding="utf-8")
        return store

    with FORUM_DB_PATH.open("r", encoding="utf-8") as f:
        try:
            store = json.load(f)
        except json.JSONDecodeError:
            store = {"next_post_id": 1, "posts": []}
    if "posts" not in store or not isinstance(store["posts"], list):
        store["posts"] = []
    if "next_post_id" not in store or not isinstance(store["next_post_id"], int):
        store["next_post_id"] = len(store["posts"]) + 1
    for post in store["posts"]:
        post.setdefault("liked_by", [])
        post.setdefault("comments", [])
        post.setdefault("visibility", "public")
        post.setdefault("club", None)
        post.setdefault("club_id", None)
        post.setdefault("genre", None)
        post.setdefault("book_id", None)
        post.setdefault("book_title", None)
        post.setdefault("tags", [])
        for c in post["comments"]:
            c.setdefault("liked_by", [])
            c.setdefault("likes", 0)
    return store


def save_forum_store(store: dict) -> None:
    """Persist forum posts/comments store to disk."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    FORUM_DB_PATH.write_text(json.dumps(store, indent=2), encoding="utf-8")

