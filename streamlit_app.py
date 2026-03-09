"""Bookish Streamlit application backed by data/processed files."""

# pylint: disable=line-too-long,too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks

from __future__ import annotations

import ast
import csv
import html
import json
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

BASE_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
USER_DB_PATH = PROCESSED_DIR / "user_accounts.json"
FORUM_DB_PATH = PROCESSED_DIR / "forum_posts.json"


def inject_styles() -> None:
    """Inject custom CSS styles for spacing and visual consistency."""
    st.markdown(
        """
        <style>
        .block-container {padding-top: 3.75rem; max-width: 1100px;}
        .stTabs [data-baseweb="tab-list"] {
            margin-top: 0.5rem;
            position: relative;
            z-index: 2;
            background: transparent;
        }
        .pill {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 999px;
            border: 1px solid #d7e5d7;
            background: #f4f8f4;
            color: #4b6f4b;
            font-size: 0.75rem;
            margin-right: 0.35rem;
            margin-top: 0.25rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _read_jsonl_dict_lines(path: Path) -> list[dict]:
    """Read JSONL file where each line is a dictionary and return parsed rows."""
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _read_isbn_index_file(path: Path) -> set[str]:
    """Read indexed ISBN JSON format and return normalized ISBN set."""
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    out = set()
    for row in data:
        val = str(row.get("0", "")).strip()
        if val:
            out.add(val.upper())
    return out


def _parse_tags(text: str) -> list[str]:
    """Parse serialized tag list string into normalized lowercase tags."""
    if not text:
        return []
    try:
        raw = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return []
    if isinstance(raw, list):
        return [str(x).strip().lower() for x in raw if str(x).strip()]
    return []


def load_user_store() -> dict:
    """Load user account store from JSON, creating a default file if needed."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    if not USER_DB_PATH.exists():
        default_store = {"users": {}}
        USER_DB_PATH.write_text(json.dumps(default_store, indent=2), encoding="utf-8")
        return default_store

    with USER_DB_PATH.open("r", encoding="utf-8") as f:
        try:
            store = json.load(f)
        except json.JSONDecodeError:
            store = {"users": {}}
    if "users" not in store or not isinstance(store["users"], dict):
        store = {"users": {}}
    return store


def save_user_store(store: dict) -> None:
    """Persist user account store to disk."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    USER_DB_PATH.write_text(json.dumps(store, indent=2), encoding="utf-8")


def ensure_user_schema(user_record: dict) -> dict:
    """Ensure user record has all expected keys for app features."""
    user_record.setdefault("name", "")
    user_record.setdefault("password", "")
    user_record.setdefault(
        "library",
        {
            "in_progress": [],
            "saved": [],
            "finished": [],
        },
    )
    user_record.setdefault("club_ids", [])
    user_record.setdefault("forum_posts", [])
    user_record.setdefault("saved_forum_post_ids", [])
    return user_record


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
        for c in post["comments"]:
            c.setdefault("liked_by", [])
            c.setdefault("likes", 0)
    return store


def save_forum_store(store: dict) -> None:
    """Persist forum posts/comments store to disk."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    FORUM_DB_PATH.write_text(json.dumps(store, indent=2), encoding="utf-8")


@st.cache_data
def load_data() -> dict:
    """Load and transform processed datasets into UI-ready structures."""
    books_parent = _read_jsonl_dict_lines(PROCESSED_DIR / "first_100_books_by_parent_asin.jsonl")
    catalog_isbns = _read_isbn_index_file(PROCESSED_DIR / "first_100_spl_catalog_by_isbn.json")
    checkout_isbns = _read_isbn_index_file(PROCESSED_DIR / "first_100_spl_checkouts_by_isbn.json")

    books = []
    for idx, row in enumerate(books_parent, start=1):
        source_id, meta = next(iter(row.items()))
        cats = meta.get("categories") or []
        genres = [str(c) for c in cats[:3]] or ["General"]
        desc = meta.get("description") or []
        description = " ".join(str(x) for x in desc[:3]).strip() if isinstance(desc, list) else str(desc).strip()
        if not description:
            description = "No description available."

        cover = meta.get("images") or "https://placehold.co/220x330?text=Book"
        rating_number = int(meta.get("rating_number") or 0)
        rating = float(meta.get("average_rating") or 0.0)
        in_spl = source_id.upper() in catalog_isbns or source_id.upper() in checkout_isbns
        books.append(
            {
                "id": idx,
                "source_id": source_id,
                "title": str(meta.get("title") or "Untitled"),
                "author": str(meta.get("author_name") or "Unknown"),
                "cover": cover,
                "rating": round(rating, 1),
                "rating_count": rating_number,
                "genres": genres,
                "description": description,
                "spl_available": in_spl,
            }
        )

    books = books[:36]
    books_by_id = {b["id"]: b for b in books}
    title_author_to_id = {f"{b['title'].strip().lower()}|{b['author'].strip().lower()}": b["id"] for b in books}

    clubs = []
    clubs_path = PROCESSED_DIR / "bookclubs_seattle_clean.csv"
    if clubs_path.exists():
        with clubs_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader, start=1):
                tags = _parse_tags(row.get("tags", ""))
                genre = tags[0].title() if tags else "General"
                key = f"{(row.get('book_title') or '').strip().lower()}|{(row.get('book_author') or '').strip().lower()}"
                current_book_id = title_author_to_id.get(key)
                current_book_title = (
                    books_by_id[current_book_id]["title"]
                    if current_book_id in books_by_id
                    else "NA"
                )
                clubs.append(
                    {
                        "id": idx,
                        "name": row.get("title") or f"Book Club {idx}",
                        "description": (row.get("description") or "No description provided.").strip(),
                        "genre": genre,
                        "location": row.get("city_state") or "Seattle, WA",
                        "meeting_day": row.get("day_of_week_start") or "TBD",
                        "meeting_time": row.get("start_time") or "TBD",
                        "current_book_id": current_book_id,
                        "current_book_title": current_book_title,
                        "thumbnail": row.get("thumbnail") or "https://placehold.co/600x360?text=Club",
                        "is_external": True,
                        "external_link": row.get("link") or "",
                    }
                )
                if idx >= 24:
                    break

    if not clubs:
        clubs = [{"id": 1, "name": "Seattle Readers", "description": "Fallback club generated because processed club data is missing.", "genre": "General", "location": "Seattle, WA", "meeting_day": "Wed", "meeting_time": "7:00 PM", "current_book_id": books[0]["id"], "current_book_title": books[0]["title"], "thumbnail": "https://placehold.co/600x360?text=Club", "is_external": False, "external_link": ""}]

    user_club_ids = [c["id"] for c in clubs[: min(4, len(clubs))]]
    library = {"in_progress": [b["id"] for b in books[0:4]], "saved": [b["id"] for b in books[4:8]], "finished": [b["id"] for b in books[8:12]]}
    forum_posts = [
        {"title": f"What do you think about {books[0]['title']}?", "author": "Community Mod", "genre": books[0]["genres"][0], "club": clubs[0]["name"] if clubs else None, "replies": 8, "likes": 15, "time_ago": "2 hours ago", "preview": f"Share your thoughts about {books[0]['title']} by {books[0]['author']}."},
        {"title": f"Top picks this week: {books[1]['title']}", "author": "Bookish Team", "genre": books[1]["genres"][0], "club": None, "replies": 5, "likes": 12, "time_ago": "1 day ago", "preview": f"This week's recommendation highlight is {books[1]['title']}."},
    ]
    genres = sorted({g for b in books for g in b["genres"]})
    neighborhoods = sorted({(c["location"].split(",", maxsplit=1)[0]).strip() for c in clubs})

    return {"books": books, "books_by_id": books_by_id, "clubs": clubs, "forum_posts": forum_posts, "genres": genres, "library": library, "neighborhoods": neighborhoods, "user_club_ids": user_club_ids}


def init_session(books: list[dict]) -> None:
    """Initialize required Streamlit session-state defaults."""
    st.session_state.setdefault("signed_in", False)
    st.session_state.setdefault("user_email", "")
    st.session_state.setdefault("user_name", "")
    st.session_state.setdefault("selected_book_id", books[0]["id"])
    st.session_state.setdefault("jump_to_book_detail", False)
    st.session_state.setdefault("selected_forum_post_id", None)
    st.session_state.setdefault("jump_to_forum_detail", False)


def handle_query_navigation(books_by_id: dict[int, dict], forum_post_ids: set[int]) -> None:
    """Handle deep-link query params for book detail and forum detail navigation."""
    book_param = st.query_params.get("book_id")
    if st.query_params.get("open") != "detail" or not book_param:
        post_param = st.query_params.get("post_id")
        if st.query_params.get("open") == "forum" and post_param:
            try:
                post_id = int(post_param)
            except (TypeError, ValueError):
                return
            if post_id in forum_post_ids:
                st.session_state["selected_forum_post_id"] = post_id
                st.session_state["jump_to_forum_detail"] = True
                st.query_params.clear()
                st.rerun()
        return
    try:
        book_id = int(book_param)
    except (TypeError, ValueError):
        return
    if book_id in books_by_id:
        st.session_state["selected_book_id"] = book_id
        st.session_state["jump_to_book_detail"] = True
        st.query_params.clear()
        st.rerun()


def auth_panel() -> None:
    """Render account panel and handle sign-in/sign-up/sign-out flows."""
    st.sidebar.subheader("Account")
    if st.session_state["signed_in"]:
        st.sidebar.success(f"Signed in as {st.session_state['user_name']}")
        if st.sidebar.button("Sign out"):
            st.session_state["signed_in"] = False
            st.session_state["user_email"] = ""
            st.session_state["user_name"] = ""
            st.rerun()
        return
    with st.sidebar.form("sign_in_form"):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        c1, c2 = st.columns(2)
        sign_in = c1.form_submit_button("Sign in")
        sign_up = c2.form_submit_button("Create Account")
    if not (sign_in or sign_up):
        return

    email = email.strip().lower()
    password = password.strip()
    if not email or not password:
        st.sidebar.error("Please enter email and password.")
        return

    store = load_user_store()
    users = store["users"]
    if sign_up:
        if email in users:
            st.sidebar.error("Account already exists. Please sign in.")
            return
        users[email] = ensure_user_schema(
            {
                "name": email.split("@")[0],
                "password": password,
            }
        )
        save_user_store(store)
        st.session_state["signed_in"] = True
        st.session_state["user_email"] = email
        st.session_state["user_name"] = users[email]["name"]
        st.sidebar.success("Account created and signed in.")
        st.rerun()

    # sign in flow
    record = users.get(email)
    if not record or record.get("password") != password:
        st.sidebar.error("Invalid email or password.")
        return
    ensure_user_schema(record)
    st.session_state["signed_in"] = True
    st.session_state["user_email"] = email
    st.session_state["user_name"] = record.get("name", email.split("@")[0])
    st.sidebar.success("Signed in.")
    st.rerun()


def render_book_card(book: dict, key_prefix: str) -> None:
    """Render book card with clickable metadata and detail action."""
    href = f"?book_id={book['id']}&open=detail"
    stats = f"Rating: {book['rating']} ({book['rating_count']:,})"
    st.markdown(f'<a href="{href}" target="_self"><img src="{book["cover"]}" alt="{html.escape(book["title"])}" style="width:145px;max-width:100%;border-radius:8px;" /></a>', unsafe_allow_html=True)
    st.markdown(f'<a href="{href}" target="_self" style="text-decoration:none;color:inherit;"><strong>{html.escape(book["title"])}</strong></a>', unsafe_allow_html=True)
    st.markdown(f'<a href="{href}" target="_self" style="text-decoration:none;color:inherit;">{html.escape(book["author"])}</a>', unsafe_allow_html=True)
    st.markdown(f'<a href="{href}" target="_self" style="text-decoration:none;color:inherit;">{html.escape(stats)}</a>', unsafe_allow_html=True)
    st.markdown("".join([f"<span class='pill'>{html.escape(g)}</span>" for g in book["genres"]]), unsafe_allow_html=True)
    if st.button("View details", key=f"{key_prefix}_details_{book['id']}"):
        st.session_state["selected_book_id"] = book["id"]
        st.session_state["jump_to_book_detail"] = True
        st.rerun()


def can_view_forum_post(post: dict, current_user: dict | None) -> bool:
    """Return whether the current user can view the given forum post."""
    if post.get("visibility") != "club":
        return True
    if current_user is None:
        return False
    club_id = post.get("club_id")
    if club_id is None:
        return bool(post.get("club"))
    return club_id in current_user.get("club_ids", [])


def main() -> None:
    """Run the Streamlit app entrypoint and render all tabs."""
    st.set_page_config(page_title="Bookish", page_icon="ðŸ“š", layout="wide")
    inject_styles()
    data = load_data()
    books = data["books"]
    books_by_id = data["books_by_id"]
    clubs = data["clubs"]
    genres = data["genres"]
    neighborhoods = data["neighborhoods"]
    forum_posts = data["forum_posts"]

    init_session(books)
    st.sidebar.title("Bookish")
    auth_panel()
    store = load_user_store()
    users = store["users"]
    current_user = None
    if st.session_state["signed_in"]:
        email = st.session_state["user_email"]
        current_user = users.get(email)
        if current_user is None:
            st.session_state["signed_in"] = False
            st.session_state["user_email"] = ""
            st.session_state["user_name"] = ""
            st.rerun()
        current_user = ensure_user_schema(current_user)

    forum_store = load_forum_store(forum_posts)
    forum_posts_data = forum_store["posts"]
    forum_post_ids = {int(p["id"]) for p in forum_posts_data if "id" in p}

    tabs = st.tabs(["Feed", "Explore Clubs", "My Clubs", "Library", "Book Detail", "Forum"])
    handle_query_navigation(books_by_id, forum_post_ids)
    if st.session_state.get("jump_to_book_detail"):
        components.html("""<script>for(const t of window.parent.document.querySelectorAll('button[role="tab"]')){if(t.textContent.trim()==="Book Detail"){t.click();break;}}</script>""", height=0)
        st.session_state["jump_to_book_detail"] = False
    if st.session_state.get("jump_to_forum_detail"):
        components.html("""<script>for(const t of window.parent.document.querySelectorAll('button[role="tab"]')){if(t.textContent.trim()==="Forum"){t.click();break;}}</script>""", height=0)
        st.session_state["jump_to_forum_detail"] = False

    with tabs[0]:
        st.title("Discover your next read")
        selected_genres = st.multiselect("Filter by genre", genres)
        filtered = [b for b in books if not selected_genres or any(g in selected_genres for g in b["genres"])]
        trending_source = filtered if selected_genres else books
        trending = sorted(trending_source, key=lambda b: b["rating_count"], reverse=True)[:4]
        st.subheader("Trending in Seattle")
        if trending:
            cols = st.columns(4)
            for i, book in enumerate(trending):
                with cols[i]:
                    render_book_card(book, f"trend_{i}")
        else:
            st.caption("No trending books match this genre filter.")
        st.subheader("Recommended for you")
        cols = st.columns(3)
        for i, book in enumerate(filtered):
            with cols[i % 3]:
                render_book_card(book, f"rec_{i}")

        st.subheader("Suggested book clubs")
        clubs_source = clubs
        if selected_genres:
            clubs_source = [
                c for c in clubs_source if c.get("genre", "").lower() in {g.lower() for g in selected_genres}
            ]
        top_clubs = clubs_source[:5]
        if not top_clubs:
            st.caption("No suggested clubs for this filter.")
        for club in top_clubs:
            st.markdown(f"**{club['name']}**")
            st.caption(
                f"{club.get('genre', 'General')} | {club.get('location', 'Seattle, WA')}"
            )
            st.write((club.get("description", "") or "")[:180] + ("..." if len(club.get("description", "")) > 180 else ""))
            if club.get("external_link"):
                st.link_button("Open club", club["external_link"], use_container_width=False)
            st.divider()

    with tabs[1]:
        st.title("Explore Clubs")
        search = st.text_input("Search clubs")
        gfilter = st.selectbox("Genre", ["All"] + genres)
        nfilter = st.selectbox("Neighborhood", ["All"] + neighborhoods)
        filtered = clubs
        if search.strip():
            q = search.strip().lower()
            filtered = [c for c in filtered if q in c["name"].lower() or q in c["description"].lower()]
        if gfilter != "All":
            filtered = [c for c in filtered if c["genre"] == gfilter]
        if nfilter != "All":
            filtered = [c for c in filtered if nfilter.lower() in c["location"].lower()]
        for club in filtered:
            st.subheader(club["name"])
            st.caption(f"{club['genre']} | {club['location']}")
            summary = club["description"][:280] + ("..." if len(club["description"]) > 280 else "")
            st.write(summary)
            st.write(f"Meetings: {club['meeting_day']} at {club['meeting_time']}")
            if club.get("external_link"):
                st.link_button("Open club listing", club["external_link"], use_container_width=False)
            if st.session_state["signed_in"] and current_user is not None:
                joined = club["id"] in current_user["club_ids"]
                if joined:
                    st.success("Joined")
                elif st.button("Join club", key=f"join_club_{club['id']}"):
                    current_user["club_ids"].append(club["id"])
                    save_user_store(store)
                    st.rerun()
            else:
                st.caption("Sign in to join clubs.")
            st.divider()

    with tabs[2]:
        st.title("My Clubs")
        if not st.session_state["signed_in"] or current_user is None:
            st.info("Sign in to see your clubs.")
        for club in [c for c in clubs if c["id"] in (current_user["club_ids"] if current_user else [])]:
            st.subheader(club["name"])
            st.caption(f"{club['location']}")
            btn_col_1, btn_col_2 = st.columns([1, 1])
            if btn_col_1.button("Details", key=f"details_club_{club['id']}"):
                toggle_key = f"show_club_details_{club['id']}"
                st.session_state[toggle_key] = not st.session_state.get(toggle_key, False)
                st.rerun()
            if current_user is not None and btn_col_2.button("Remove Club", key=f"remove_club_{club['id']}"):
                current_user["club_ids"] = [
                    cid for cid in current_user.get("club_ids", []) if int(cid) != int(club["id"])
                ]
                save_user_store(store)
                st.success(f"Removed {club['name']} from My Clubs.")
                st.rerun()

            if st.session_state.get(f"show_club_details_{club['id']}", False):
                st.markdown(f"**Genre:** {club.get('genre', 'General')}")
                st.markdown(
                    f"**Meeting:** {club.get('meeting_day', 'TBD')} at {club.get('meeting_time', 'TBD')}"
                )
                st.write(club.get("description", ""))
            st.divider()
        if st.session_state["signed_in"] and current_user is not None and not current_user["club_ids"]:
            st.info("You have not joined any clubs yet.")

    with tabs[3]:
        st.title("Library")
        if not st.session_state["signed_in"] or current_user is None:
            st.info("Sign in to see your books.")
        else:
            user_library = current_user["library"]
            ltabs = st.tabs(["Saved", "In Progress", "Finished"])
            for key, tab in zip(["saved", "in_progress", "finished"], ltabs):
                with tab:
                    book_ids = [bid for bid in user_library[key] if bid in books_by_id]
                    if not book_ids:
                        st.caption("No books in this list yet.")
                        continue
                    cols = st.columns(3)
                    for i, bid in enumerate(book_ids):
                        with cols[i % 3]:
                            render_book_card(books_by_id[bid], f"{key}_{i}")

    with tabs[4]:
        st.title("Book Detail")
        if st.session_state["selected_book_id"] not in books_by_id:
            st.session_state["selected_book_id"] = books[0]["id"]
        book = books_by_id[st.session_state["selected_book_id"]]
        c1, c2 = st.columns([1, 2])
        with c1:
            st.image(book["cover"], width="stretch")
        with c2:
            st.subheader(book["title"])
            st.caption(book["author"])
            st.write(f"Rating: **{book['rating']}** ({book['rating_count']:,})")
            st.write(book["description"])
            save_option = st.selectbox(
                "Save to library as",
                ["Saved", "In Progress", "Finished"],
                disabled=not st.session_state["signed_in"],
            )
            if st.button("Update status", disabled=not st.session_state["signed_in"]):
                if current_user is None:
                    st.warning("Sign in to save books.")
                else:
                    status_key_map = {
                        "Saved": "saved",
                        "In Progress": "in_progress",
                        "Finished": "finished",
                    }
                    target_key = status_key_map[save_option]
                    for key in ["saved", "in_progress", "finished"]:
                        current_user["library"][key] = [
                            bid for bid in current_user["library"][key] if bid != book["id"]
                        ]
                    current_user["library"][target_key].append(book["id"])
                    save_user_store(store)
                    st.success(f"Saved to {save_option}.")
            if not st.session_state["signed_in"]:
                st.caption("Sign in to save books.")

    with tabs[5]:
        st.title("Forum")
        if st.session_state.get("selected_forum_post_id") is not None:
            selected_post = next(
                (p for p in forum_posts_data if int(p.get("id", -1)) == int(st.session_state["selected_forum_post_id"])),
                None,
            )
            if selected_post is not None and can_view_forum_post(selected_post, current_user):
                st.markdown("### Discussion")
                if st.button("Back to posts"):
                    st.session_state["selected_forum_post_id"] = None
                    st.rerun()
                st.markdown(f"## {selected_post['title']}")
                tags = [x for x in [selected_post.get("genre"), selected_post.get("club")] if x]
                st.caption(f"{selected_post['author']} | {selected_post['time_ago']} | {' | '.join(tags)}")
                st.write(selected_post["preview"])

                # Post actions
                a1, a2 = st.columns(2)
                if current_user is not None:
                    email = st.session_state["user_email"]
                    liked_by = selected_post.get("liked_by", [])
                    liked = email in liked_by
                    if a1.button("Unlike post" if liked else "Like post", key=f"like_post_{selected_post['id']}"):
                        if liked:
                            selected_post["liked_by"] = [u for u in liked_by if u != email]
                            selected_post["likes"] = max(0, int(selected_post.get("likes", 0)) - 1)
                        else:
                            selected_post.setdefault("liked_by", []).append(email)
                            selected_post["likes"] = int(selected_post.get("likes", 0)) + 1
                        save_forum_store(forum_store)
                        st.rerun()
                    saved_ids = current_user.get("saved_forum_post_ids", [])
                    is_saved = int(selected_post["id"]) in saved_ids
                    if a2.button("Unsave post" if is_saved else "Save post", key=f"save_post_{selected_post['id']}"):
                        if is_saved:
                            current_user["saved_forum_post_ids"] = [
                                pid for pid in saved_ids if int(pid) != int(selected_post["id"])
                            ]
                        else:
                            current_user["saved_forum_post_ids"].append(int(selected_post["id"]))
                        save_user_store(store)
                        st.rerun()
                else:
                    a1.caption("Sign in to like posts.")
                    a2.caption("Sign in to save posts.")

                st.caption(f"Likes: {selected_post.get('likes', 0)}")
                st.markdown("#### Comments")
                comments = selected_post.get("comments", [])
                if not comments:
                    st.caption("No comments yet.")
                for idx, comment in enumerate(comments):
                    st.markdown(f"**{comment.get('author','User')}**")
                    st.write(comment.get("text", ""))
                    if current_user is not None:
                        email = st.session_state["user_email"]
                        c_liked_by = comment.get("liked_by", [])
                        c_liked = email in c_liked_by
                        if st.button(
                            f"{'Unlike' if c_liked else 'Like'} comment ({comment.get('likes', 0)})",
                            key=f"like_comment_{selected_post['id']}_{idx}",
                        ):
                            if c_liked:
                                comment["liked_by"] = [u for u in c_liked_by if u != email]
                                comment["likes"] = max(0, int(comment.get("likes", 0)) - 1)
                            else:
                                comment.setdefault("liked_by", []).append(email)
                                comment["likes"] = int(comment.get("likes", 0)) + 1
                            save_forum_store(forum_store)
                            st.rerun()
                    else:
                        st.caption(f"Likes: {comment.get('likes', 0)}")
                    st.divider()

                if current_user is not None:
                    with st.form(f"reply_form_{selected_post['id']}"):
                        reply = st.text_area("Write a reply")
                        submit_reply = st.form_submit_button("Reply")
                    if submit_reply:
                        if reply.strip():
                            selected_post.setdefault("comments", []).append(
                                {
                                    "author": st.session_state["user_name"],
                                    "text": reply.strip(),
                                    "likes": 0,
                                    "liked_by": [],
                                }
                            )
                            selected_post["replies"] = len(selected_post.get("comments", []))
                            save_forum_store(forum_store)
                            st.rerun()
                        else:
                            st.warning("Please write a reply before submitting.")
                else:
                    st.caption("Sign in to reply to comments.")
            else:
                st.session_state["selected_forum_post_id"] = None

        if st.session_state.get("selected_forum_post_id") is None:
            if st.session_state["signed_in"] and current_user is not None:
                joined_clubs = [
                    c for c in clubs if c["id"] in current_user.get("club_ids", [])
                ]
                with st.form("new_forum_post"):
                    st.subheader("Create a forum post")
                    post_title = st.text_input("Title")
                    post_text = st.text_area("Post")
                    c1, c2 = st.columns(2)
                    visibility = c1.selectbox(
                        "Visibility",
                        ["Public", "Club Members"],
                        help="Public posts are visible to everyone. Club posts are only for one club.",
                    )
                    selected_club_name = None
                    selected_club_id = None
                    if visibility == "Club Members":
                        club_options = [f"{c['id']}::{c['name']}" for c in joined_clubs]
                        selected_club_name = c2.selectbox(
                            "Club",
                            [c.split("::", 1)[1] for c in club_options] if club_options else ["No joined clubs"],
                            disabled=not club_options,
                        )
                        if club_options:
                            selected_club_id = [
                                int(c.split("::", 1)[0])
                                for c in club_options
                                if c.split("::", 1)[1] == selected_club_name
                            ][0]
                    submitted = st.form_submit_button("Post")
                if submitted:
                    if post_title.strip() and post_text.strip():
                        if visibility == "Club Members" and not joined_clubs:
                            st.warning("Join a club first to create club-only posts.")
                        else:
                            forum_store["posts"].insert(
                                0,
                                {
                                    "id": int(forum_store["next_post_id"]),
                                    "title": post_title.strip(),
                                    "author": st.session_state["user_name"],
                                    "genre": None,
                                    "club": selected_club_name if visibility == "Club Members" else None,
                                    "club_id": selected_club_id if visibility == "Club Members" else None,
                                    "visibility": "club" if visibility == "Club Members" else "public",
                                    "replies": 0,
                                    "likes": 0,
                                    "liked_by": [],
                                    "time_ago": "just now",
                                    "preview": post_text.strip(),
                                    "comments": [],
                                },
                            )
                            forum_store["next_post_id"] = int(forum_store["next_post_id"]) + 1
                            save_forum_store(forum_store)
                            st.success("Posted to forum.")
                            st.rerun()
                    else:
                        st.warning("Please add both title and post content.")
            else:
                st.caption("Sign in to create and save forum posts.")

            view = st.radio(
                "View",
                ["All", "Public", "Club Discussions", "Saved"],
                horizontal=True,
            )
            posts = [p for p in forum_posts_data if can_view_forum_post(p, current_user)]
            if view == "Public":
                posts = [p for p in posts if p.get("visibility") != "club"]
            elif view == "Club Discussions":
                posts = [p for p in posts if p.get("visibility") == "club"]
            elif view == "Saved":
                if current_user is None:
                    posts = []
                else:
                    saved_ids = {int(pid) for pid in current_user.get("saved_forum_post_ids", [])}
                    posts = [p for p in posts if int(p.get("id", -1)) in saved_ids]

            if view == "Saved" and current_user is None:
                st.info("Sign in to view saved forum posts.")
            elif view == "Saved" and not posts:
                st.caption("No saved posts yet.")

            for post in posts:
                st.markdown(f"### {post['title']}")
                tags = [x for x in [post.get("genre"), post.get("club")] if x]
                meta = f"{post['author']} | {post['time_ago']} | {' | '.join(tags)}"
                st.caption(meta)
                st.write(post["preview"])
                st.caption(f"Likes: {int(post.get('likes',0))} | Replies: {int(post.get('replies',0))}")
                if st.button("Open discussion", key=f"open_forum_post_{int(post['id'])}"):
                    st.session_state["selected_forum_post_id"] = int(post["id"])
                    st.rerun()
                st.divider()


if __name__ == "__main__":
    main()
