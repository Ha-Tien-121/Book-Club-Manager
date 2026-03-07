"""Bookish Streamlit application."""

import streamlit as st

from app.mock_data import (
    books,
    books_by_id,
    clubs,
    forum_posts,
    genres,
    library,
    neighborhoods,
    user_club_ids,
)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1.25rem; max-width: 1100px;}
        .book-card {
            border: 1px solid #e7dfd1;
            border-radius: 14px;
            background: #fbf8f3;
            padding: 0.9rem;
            margin-bottom: 0.75rem;
        }
        .club-card {
            border: 1px solid #e7dfd1;
            border-radius: 14px;
            background: #fefdfb;
            padding: 1rem;
            margin-bottom: 0.9rem;
        }
        .muted {color: #7f7468; font-size: 0.92rem;}
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


def init_session() -> None:
    st.session_state.setdefault("signed_in", False)
    st.session_state.setdefault("user_name", "")
    st.session_state.setdefault("selected_book_id", books[0]["id"])
    st.session_state.setdefault("nav_page", "Feed")


def auth_panel() -> None:
    st.sidebar.subheader("Account")
    if st.session_state["signed_in"]:
        st.sidebar.success(f"Signed in as {st.session_state['user_name']}")
        if st.sidebar.button("Sign out"):
            st.session_state["signed_in"] = False
            st.session_state["user_name"] = ""
            st.rerun()
        return

    with st.sidebar.form("sign_in_form"):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in")
    if submitted:
        if email.strip() and password.strip():
            st.session_state["signed_in"] = True
            st.session_state["user_name"] = email.split("@")[0]
            st.sidebar.success("Signed in")
            st.rerun()
        else:
            st.sidebar.error("Please enter email and password.")


def render_book_card(book: dict, key_prefix: str) -> None:
    st.image(book["cover"], width=145)
    st.markdown(f"**{book['title']}**")
    st.caption(book["author"])
    st.caption(
        f"Rating: {book['rating']} ({book['rating_count']:,}) | Clubs: {book['clubs_reading']}"
    )
    st.markdown("".join([f"<span class='pill'>{g}</span>" for g in book["genres"]]), unsafe_allow_html=True)
    if st.button("View details", key=f"{key_prefix}_details_{book['id']}"):
        st.session_state["selected_book_id"] = book["id"]
        st.session_state["nav_page"] = "Book Detail"
        st.rerun()


def feed_page() -> None:
    st.title("Discover your next read")
    st.write(
        "Personalized book recommendations based on your preferences and Seattle reading trends."
    )
    selected_genres = st.multiselect("Filter by genre", genres)
    filtered = books
    if selected_genres:
        filtered = [b for b in books if any(g in selected_genres for g in b["genres"])]

    trending = sorted(books, key=lambda b: b["checkouts"], reverse=True)[:4]
    st.subheader("Trending in Seattle")
    trend_cols = st.columns(4)
    for idx, book in enumerate(trending):
        with trend_cols[idx]:
            render_book_card(book, key_prefix=f"trend_{idx}")

    st.subheader("Recommended for you")
    rec_cols = st.columns(3)
    for idx, book in enumerate(filtered):
        with rec_cols[idx % 3]:
            render_book_card(book, key_prefix=f"rec_{idx}")


def explore_clubs_page() -> None:
    st.title("Explore Clubs")
    st.write("Find active clubs by genre, location, and availability.")
    search = st.text_input("Search clubs")
    genre_filter = st.selectbox("Genre", ["All"] + genres)
    neighborhood_filter = st.selectbox("Neighborhood", ["All"] + neighborhoods)

    filtered = clubs
    if search.strip():
        q = search.strip().lower()
        filtered = [
            c
            for c in filtered
            if q in c["name"].lower() or q in c["description"].lower()
        ]
    if genre_filter != "All":
        filtered = [c for c in filtered if c["genre"] == genre_filter]
    if neighborhood_filter != "All":
        filtered = [
            c for c in filtered if neighborhood_filter.lower() in c["location"].lower()
        ]

    if not filtered:
        st.info("No clubs match these filters.")
        return

    for club in filtered:
        current = books_by_id[club["current_book_id"]]
        st.markdown("<div class='club-card'>", unsafe_allow_html=True)
        c1, c2 = st.columns([1, 2.3])
        with c1:
            st.image(club["thumbnail"], width="stretch")
        with c2:
            st.markdown(f"### {club['name']}")
            st.caption(f"{club['genre']} | {club['location']}")
            st.write(club["description"])
            st.markdown(
                f"**Meetings:** {club['meeting_day']} at {club['meeting_time']} | "
                f"**Members:** {club['members']}"
            )
            st.markdown(f"**Currently reading:** {current['title']}")
            st.button("Join", key=f"join_{club['id']}")
        st.markdown("</div>", unsafe_allow_html=True)


def my_clubs_page() -> None:
    st.title("My Clubs")
    my_clubs = [c for c in clubs if c["id"] in user_club_ids]
    for club in my_clubs:
        book = books_by_id[club["current_book_id"]]
        st.markdown(f"### {club['name']}")
        st.caption(f"{club['members']} members | {club['location']}")
        st.write(f"Currently reading: **{book['title']}**")
        st.divider()


def library_page() -> None:
    st.title("Library")
    tabs = st.tabs(["In Progress", "Saved", "Finished"])
    tab_to_key = [("in_progress", tabs[0]), ("saved", tabs[1]), ("finished", tabs[2])]
    for key, tab in tab_to_key:
        with tab:
            ids = library[key]
            if not ids:
                st.info("No books in this category yet.")
                continue
            cols = st.columns(3)
            for idx, book_id in enumerate(ids):
                with cols[idx % 3]:
                    render_book_card(books_by_id[book_id], key_prefix=f"{key}_{idx}")


def book_detail_page() -> None:
    st.title("Book Detail")
    options = {f"{b['title']} - {b['author']}": b["id"] for b in books}
    selected_label = st.selectbox(
        "Select a book",
        options=list(options.keys()),
        index=list(options.values()).index(st.session_state["selected_book_id"]),
    )
    st.session_state["selected_book_id"] = options[selected_label]
    book = books_by_id[st.session_state["selected_book_id"]]

    c1, c2 = st.columns([1, 2])
    with c1:
        st.image(book["cover"], width="stretch")
    with c2:
        st.subheader(book["title"])
        st.caption(book["author"])
        st.write(
            f"Rating: **{book['rating']}** ({book['rating_count']:,} ratings)  "
            f"|  Clubs reading: **{book['clubs_reading']}**"
        )
        st.write(book["description"])
        status = st.selectbox("Save to library as", ["Saved", "In Progress", "Finished"])
        st.button(f"Update status to {status}")

    if book["spl_available"]:
        st.markdown("#### Seattle Public Library availability")
        st.write(", ".join(book["spl_branches"]))
        st.caption(f"Checked out {book['checkouts']:,} times in the past year")


def forum_page() -> None:
    st.title("Forum")
    view = st.radio("View", ["All", "Public", "Club Discussions"], horizontal=True)
    posts = forum_posts
    if view == "Public":
        posts = [p for p in posts if not p["club"]]
    elif view == "Club Discussions":
        posts = [p for p in posts if p["club"]]

    for post in posts:
        st.markdown("### " + post["title"])
        tags = []
        if post["genre"]:
            tags.append(post["genre"])
        if post["club"]:
            tags.append(post["club"])
        st.caption(f"{post['author']} | {post['time_ago']} | {' | '.join(tags)}")
        st.write(post["preview"])
        st.caption(f"Likes: {post['likes']} | Replies: {post['replies']}")
        st.divider()


def main() -> None:
    st.set_page_config(page_title="Bookish", page_icon="📚", layout="wide")
    inject_styles()
    init_session()
    auth_panel()

    st.sidebar.title("Bookish")
    page = st.sidebar.radio(
        "Navigate",
        ["Feed", "Explore Clubs", "My Clubs", "Library", "Book Detail", "Forum"],
        key="nav_page",
    )

    if page == "Feed":
        feed_page()
    elif page == "Explore Clubs":
        explore_clubs_page()
    elif page == "My Clubs":
        my_clubs_page()
    elif page == "Library":
        library_page()
    elif page == "Book Detail":
        book_detail_page()
    else:
        forum_page()


if __name__ == "__main__":
    main()
