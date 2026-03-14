"""
Important: This is all tentative but forum will be tracked here. 
I need to double check UI to see what this script needs.

Forum service: business logic for book discussion threads and replies.

Sits above the storage layer and provides operations needed by the UI and API:
fetching thread summaries, loading a full thread for a book, adding posts and
replies, and doing light moderation (e.g., hide / flag content).
"""

from typing import Any, Optional

from backend import storage


def get_thread_for_book(parent_asin: str) -> Optional[dict[str, Any]]:
    """
    Get the discussion thread associated with a book.

    Intended for the book details page.
    """
    # TODO: implement when storage.get_form_thread is ready
    return None


def list_threads_for_user(user_id: str, limit: int = 20) -> list[dict[str, Any]]:
    """
    List recent threads the user has participated in.
    """
    # TODO: implement (likely via a user_forums helper in storage)
    return []


def add_post_to_thread(
    parent_asin: str,
    user_id: str,
    content: str,
) -> dict[str, Any]:
    """
    Add a new top-level post to the thread for a book.
    """
    # TODO: implement write to forum storage (new post id, timestamps, etc.)
    return {
        "ok": True,
        "action": "add_post",
        "parent_asin": parent_asin,
        "user_id": user_id,
        "content": content,
    }


def reply_to_post(
    thread_id: str,
    post_id: str,
    user_id: str,
    content: str,
) -> dict[str, Any]:
    """
    Add a reply to an existing post within a thread.
    """
    # TODO: implement write to forum storage, linking by thread_id/post_id
    return {
        "ok": True,
        "action": "reply",
        "thread_id": thread_id,
        "post_id": post_id,
        "user_id": user_id,
        "content": content,
    }


