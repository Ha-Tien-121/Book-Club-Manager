"""Sidebar auth UI."""

from __future__ import annotations

import streamlit as st

from app.stores.user_store import (
    create_user,
    ensure_user_account_schema,
    load_user_store,
)


def auth_panel() -> None:
    """Render account panel and handle sign-in/sign-up/sign-out flows."""
    st.sidebar.subheader("Account")
    if st.session_state.get("signed_in"):
        st.sidebar.success(f"Signed in as {st.session_state.get('user_name', '')}")
        if st.sidebar.button("Sign out"):
            st.session_state["signed_in"] = False
            st.session_state["user_email"] = ""
            st.session_state["user_name"] = ""
            st.rerun()
        return

    with st.sidebar.form("sign_in_form"):
        email = st.text_input("Email", key="auth_email")
        password = st.text_input("Password", type="password", key="auth_password")
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
    users = store["accounts"].setdefault("users", {})

    if sign_up:
        if email in users:
            st.sidebar.error("Email has been taken.")
            return
        create_user(store, email=email, password=password)
        st.session_state["signed_in"] = True
        st.session_state["user_email"] = email
        st.session_state["user_name"] = users[email]["name"]
        st.session_state["show_genre_onboarding"] = True
        st.sidebar.success("Account created and signed in.")
        st.rerun()

    record = users.get(email)
    if not record or record.get("password") != password:
        st.sidebar.error("Invalid email or password.")
        return

    ensure_user_account_schema(record)
    st.session_state["signed_in"] = True
    st.session_state["user_email"] = email
    st.session_state["user_name"] = record.get("name", email.split("@")[0])
    st.sidebar.success("Signed in.")
    st.rerun()

