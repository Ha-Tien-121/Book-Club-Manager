"""Forum service layer: posts, comments, likes, and saved posts.

This module is responsible for:

- Creating and reading forum posts (all posts, by id, by tag, by book parent_asin).
- Adding comments to posts.
- Toggling likes on posts and comments (who liked is stored in user_forums).
- Toggling saved post IDs per user (user_forums.saved_forum_post_ids).

All persistence goes through the storage abstraction (get_storage()), so the same
logic works with local JSON (load_forum_db / save_forum_db) and AWS DynamoDB.
"""

from __future__ import annotations

import time

from backend.storage import get_storage


def create_post(
    user_id: str,
    title: str,
    text: str,
    tags: list[str] | None = None,
    parent_asin: str | None = None,
    book_title: str | None = None,
) -> dict:
    """Create a new public forum post and persist it.

    When created from a book detail page, pass parent_asin (and optionally book_title).
    The post will store parent_asin and book_title, and the book title is auto-added to
    tags so the post is findable by book and maps to the exact book.

    Args:
        user_id: Author email/identifier (normalized to lowercase).
        title: Post title (required, stripped).
        text: Post body/preview text (required, stripped).
        tags: Optional list of tag strings from the UI (e.g. genres, book titles).
        parent_asin: Optional book id when post is created under book details forum.
        book_title: Optional book title for display/tagging; if None and parent_asin
            is set, title is looked up from storage (get_book_metadata) when possible.

    Returns:
        The new post dict (id, title, author, parent_asin, book_title, tags, etc.).

    Raises:
        ValueError: If title or text is empty after stripping.
    """
    user_id = str(user_id).strip().lower()
    title = str(title).strip()
    text = str(text).strip()
    if not title or not text:
        raise ValueError("title and text required")
    # Normalize tags from UI: strip, drop empties, preserve first occurrence order.
    norm_tags: list[str] = []
    seen: set[str] = set()
    for raw in tags or []:
        s = str(raw).strip()
        key = s.lower()
        if s and key not in seen:
            seen.add(key)
            norm_tags.append(s)

    store = get_storage()
    # Book-context: set parent_asin and book_title; auto-tag with book title.
    pa = str(parent_asin or "").strip() or None
    bt = str(book_title or "").strip() or None
    if pa:
        if not bt:
            try:
                meta = store.get_book_metadata(pa)
                if meta and meta.get("title"):
                    bt = str(meta["title"]).strip()
            except (RuntimeError, ValueError, TypeError, KeyError):
                pass
        if bt:
            key_bt = bt.lower()
            if key_bt not in seen:
                seen.add(key_bt)
                norm_tags.append(bt)
    db = store.load_forum_db()
    post_id = int(db.get("next_post_id") or 1)
    post = {
        "id": post_id,
        "title": title,
        "author": user_id,
        "parent_asin": pa,
        "book_title": bt,
        "tags": norm_tags,
        "replies": 0,
        "likes": 0,
        "liked_by": [],
        "created_at": int(time.time()),
        "text": text,
        "comments": [],
    }
    db.setdefault("posts", []).insert(0, post)
    db["next_post_id"] = post_id + 1
    store.save_forum_db(db)
    return dict(post)


def add_comment(post_id: int, user_id: str, text: str) -> dict:
    """Append a comment to a post and update reply count.

    Args:
        post_id: Post id.
        user_id: Comment author email/identifier.
        text: Comment body (required, stripped).

    Returns:
        Updated post dict (with new comment in comments list).

    Raises:
        ValueError: If text is empty or post not found.
    """
    user_id = str(user_id).strip().lower()
    text = str(text).strip()
    if not text:
        raise ValueError("text required")
    store = get_storage()
    post = store.get_forum_post(post_id)
    if not post:
        raise ValueError("post not found")
    comments = list(post.get("comments") or [])
    comments.append(
        {
            "author": user_id,
            "text": text,
            "likes": 0,
            "liked_by": [],
            "created_at": int(time.time()),
        }
    )
    post["comments"] = comments
    post["replies"] = len(comments)
    store.update_forum_post(post_id, post)
    return post


def like_post(post_id: int, user_id: str) -> dict:
    """Toggle the current user's like on a post.

    Who liked is stored in user_forums.liked_post_ids; the post's likes count is updated.

    Args:
        post_id: Post id.
        user_id: User email/identifier.

    Returns:
        Updated post dict (with new likes count).

    Raises:
        ValueError: If post not found.
    """
    post_id = int(post_id)
    user_id = str(user_id).strip().lower()
    store = get_storage()
    uf = store.get_user_forums(user_id)
    liked = list(uf.get("liked_post_ids") or [])
    if post_id in liked:
        liked = [x for x in liked if x != post_id]
        delta = -1
    else:
        liked = list(liked) + [post_id]
        delta = 1
    uf["liked_post_ids"] = liked
    store.save_user_forums(user_id, uf)
    post = store.get_forum_post(post_id)
    if not post:
        raise ValueError("post not found")
    post["likes"] = max(0, int(post.get("likes") or 0) + delta)
    store.update_forum_post(post_id, post)
    return post


def like_comment(post_id: int, comment_idx: int, user_id: str) -> dict:
    """Toggle the current user's like on a comment.

    Who liked is stored in user_forums.liked_comment_ids (key format post_id:comment_idx).

    Args:
        post_id: Post id.
        comment_idx: Zero-based index of the comment in the post's comments list.
        user_id: User email/identifier.

    Returns:
        Updated post dict (with new comment likes count).

    Raises:
        ValueError: If post or comment not found.
    """
    post_id = int(post_id)
    comment_idx = int(comment_idx)
    user_id = str(user_id).strip().lower()
    key = f"{post_id}:{comment_idx}"
    store = get_storage()
    uf = store.get_user_forums(user_id)
    liked = list(uf.get("liked_comment_ids") or [])
    if key in liked:
        liked = [x for x in liked if x != key]
        delta = -1
    else:
        liked = list(liked) + [key]
        delta = 1
    uf["liked_comment_ids"] = liked
    store.save_user_forums(user_id, uf)
    post = store.get_forum_post(post_id)
    if not post:
        raise ValueError("post not found")
    comments = list(post.get("comments") or [])
    if comment_idx < 0 or comment_idx >= len(comments):
        raise ValueError("comment not found")
    comments[comment_idx]["likes"] = max(0, int(comments[comment_idx].get("likes") or 0) + delta)
    post["comments"] = comments
    store.update_forum_post(post_id, post)
    return post


def save_post(post_id: int, user_id: str) -> dict:
    """Toggle whether the post is saved for the user (bookmark).

    Args:
        post_id: Post id.
        user_id: User email/identifier.

    Returns:
        Updated user_forums record (saved_forum_post_ids, liked_post_ids, liked_comment_ids).
    """
    user_id = str(user_id).strip().lower()
    store = get_storage()
    rec = store.get_user_forums(user_id)
    saved = list(rec.get("saved_forum_post_ids") or [])
    post_id_int = int(post_id)
    if post_id_int in [int(x) for x in saved]:
        saved = [x for x in saved if int(x) != post_id_int]
    else:
        saved = list(saved) + [post_id_int]
    rec["saved_forum_post_ids"] = saved
    store.save_user_forums(user_id, rec)
    return dict(rec)


def get_posts() -> list[dict]:
    """Return all forum posts, newest first.

    In AWS mode, CloudStorage.load_forum_db uses the created_at-index GSI to return
    posts ordered by created_at descending; in local mode, posts are stored newest-first.

    Returns:
        List of post dicts (id, title, author, comments, likes, etc.).
    """
    store = get_storage()
    db = store.load_forum_db()
    return list(db.get("posts") or [])


def get_post(post_id: int) -> dict:
    """Return a single forum post by id.

    Args:
        post_id: Post id.

    Returns:
        Post dict if found; empty dict if not found.
    """
    store = get_storage()
    post = store.get_forum_post(post_id)
    return dict(post) if post else {}


def get_thread_for_book(parent_asin: str) -> list[dict]:
    """Return forum posts for a book (by parent_asin or tag match).

    Uses storage's get_forum_thread_for_book (GSI when available).

    Args:
        parent_asin: Book id (matched against post parent_asin and tags).

    Returns:
        List of post dicts for that book.
    """
    parent_asin = str(parent_asin or "").strip()
    if not parent_asin:
        return []
    store = get_storage()
    return list(store.get_forum_thread_for_book(parent_asin) or [])


def filter_posts_by_tag(query: str) -> list[dict]:
    """Return posts whose tags contain the query string (case-insensitive substring).

    Args:
        query: Tag search string; empty returns all posts.

    Returns:
        List of matching post dicts.
    """
    query = str(query or "").strip().lower()
    if not query:
        return get_posts()
    out = []
    for post in get_posts():
        tags = post.get("tags") or []
        blob = " ".join(str(t) for t in tags).lower()
        if query in blob:
            out.append(post)
    return out


def get_posts_sorted(sort: str = "newest", tag: str | None = None) -> list[dict]:
    """Return posts with optional tag filter, sorted for UI (newest or top_likes).

    Args:
        sort: One of \"newest\" (default) or \"top_likes\".
        tag: Optional tag filter; case-insensitive substring match in tags.

    Returns:
        List of post dicts after filtering and sorting.
    """
    posts = get_posts()
    q = str(tag or "").strip().lower()
    if q:
        filtered: list[dict] = []
        for post in posts:
            tags = post.get("tags") or []
            blob = " ".join(str(t) for t in tags).lower()
            if q in blob:
                filtered.append(post)
        posts = filtered

    if sort == "top_likes":
        posts = sorted(
            posts,
            key=lambda p: (int(p.get("likes") or 0), int(p.get("created_at") or 0)),
            reverse=True,
        )
    # else \"newest\": get_posts() is already newest-first.
    return posts


def is_post_saved(user_id: str, post_id: int) -> bool:
    """Return True if the user has saved this post.

    Args:
        user_id: User email/identifier.
        post_id: Post id.

    Returns:
        True if post_id is in the user's saved_forum_post_ids; False otherwise.
    """
    user_id = str(user_id).strip().lower()
    post_id_int = int(post_id)
    if not user_id:
        return False
    store = get_storage()
    rec = store.get_user_forums(user_id)
    saved = rec.get("saved_forum_post_ids") or []
    return post_id_int in [int(x) for x in saved]


def is_post_liked(user_id: str, post_id: int) -> bool:
    """Return True if the user has liked this post.

    Args:
        user_id: User email/identifier.
        post_id: Post id.

    Returns:
        True if post_id is in the user's liked_post_ids; False otherwise.
    """
    user_id = str(user_id).strip().lower()
    post_id_int = int(post_id)
    if not user_id:
        return False
    store = get_storage()
    rec = store.get_user_forums(user_id)
    liked = rec.get("liked_post_ids") or []
    return post_id_int in [int(x) for x in liked]


def get_saved_posts_with_details(user_id: str) -> list[dict]:
    """Return the user's saved posts as full post dicts for UI display.

    Fetches saved post IDs, then loads each post's full details. Posts that no
    longer exist are omitted from the result. Order matches saved_forum_post_ids.

    Args:
        user_id: User email/identifier.

    Returns:
        List of post dicts (full schema), in the same order as saved.
    """
    user_id = str(user_id).strip().lower()
    if not user_id:
        return []
    store = get_storage()
    rec = store.get_user_forums(user_id)
    saved_ids = [int(x) for x in (rec.get("saved_forum_post_ids") or [])]
    out: list[dict] = []
    for pid in saved_ids:
        post = store.get_forum_post(pid)
        if post:
            out.append(post)
    return out
