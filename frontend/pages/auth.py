"""Sidebar auth UI: Sign in and link to Create account page."""

from __future__ import annotations

import streamlit as st

from backend.services.auth_service import create_user, get_user, login_user
from backend.services.library_service import update_user_preferences
from backend.services.recommender_service import refresh_and_save_recommendations
from backend.storage import get_storage


def auth_panel() -> None:
    """Render sign-in form; 'Create account' button sends user to create-account page."""
    st.sidebar.subheader("Account")
    if st.session_state.get("signed_in"):
        st.sidebar.success(f"Welcome, {st.session_state.get('user_name', '')}")
        if st.sidebar.button("Sign out"):
            st.session_state["signed_in"] = False
            st.session_state["user_email"] = ""
            st.session_state["user_name"] = ""
            st.rerun()
        return

    if st.session_state.get("show_create_account"):
        return

    st.sidebar.markdown("**Sign in**")
    with st.sidebar.form("sign_in_form"):
        signin_email = st.text_input("Email", key="auth_signin_email", placeholder="you@example.com")
        signin_password = st.text_input("Password", type="password", key="auth_signin_password")
        sign_in = st.form_submit_button("Sign in")

    if sign_in:
        email = signin_email.strip().lower()
        password = signin_password.strip()
        if not email or not password:
            st.sidebar.error("Enter email and password to sign in.")
        else:
            try:
                record = login_user(email=email, password=password)
                st.session_state["signed_in"] = True
                st.session_state["user_email"] = email
                st.session_state["user_name"] = record.get("name", email.split("@")[0])
                st.sidebar.success("Signed in.")
                st.rerun()
            except ValueError as exc:
                st.sidebar.error(str(exc))

    st.sidebar.caption("Don't have an account?")
    if st.sidebar.button("Create account", type="secondary"):
        st.session_state["show_create_account"] = True
        st.rerun()


def render_genre_onboarding(genres: list[str], current_user: dict, store: dict) -> None:
    """Show genre preference checkboxes; save to user and return to feed on Save."""
    _ = store
    st.title("Welcome! Choose your favorite genres")
    st.caption("Check the genres you most like to read.")
    current_prefs = set(current_user.get("genre_preferences") or [])
    selected: list[str] = []
    cols = st.columns(3)
    for i, genre in enumerate(sorted(genres)):
        with cols[i % 3]:
            if st.checkbox(
                genre, value=genre in current_prefs, key=f"genre_onboarding_{genre}"
            ):
                selected.append(genre)
    if st.button("Save Preferences", key="save_genre_preferences"):
        update_user_preferences(current_user["user_id"], selected)
        current_user["genre_preferences"] = selected
        try:
            refresh_and_save_recommendations(current_user["user_id"])
        except Exception:
            pass
        st.session_state["show_genre_onboarding"] = False
        st.success("Preferences saved. Taking you to the feed.")
        st.rerun()


def render_create_account_page() -> None:
    """Full-page create account form; Back returns to sign-in."""
    st.title("Create account")
    if st.button("← Back to sign in"):
        st.session_state["show_create_account"] = False
        st.rerun()

    with st.form("create_account_page_form"):
        email = st.text_input("Email", placeholder="you@example.com")
        password = st.text_input("Password", type="password")
        name = st.text_input("Display name (optional)", placeholder="Your name")
        submit = st.form_submit_button("Create account")

    if submit:
        email = (email or "").strip().lower()
        password = (password or "").strip()
        name = (name or "").strip()
        if not email or not password:
            st.error("Email and password are required.")
        else:
            try:
                create_user(email=email, password=password)
                if name:
                    store = get_storage()
                    record = store.get_user_account(email)
                    if record:
                        record["name"] = name
                        store.save_user_account(record)
                st.session_state["signed_in"] = True
                st.session_state["user_email"] = email
                user = get_user(email)
                st.session_state["user_name"] = (user or {}).get(
                    "name", name or email.split("@")[0]
                )
                st.session_state["show_create_account"] = False
                st.session_state["show_genre_onboarding"] = True
                st.success("Account created. You're signed in.")
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))
