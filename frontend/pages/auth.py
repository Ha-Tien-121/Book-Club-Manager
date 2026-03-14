"""Sidebar auth UI."""

from __future__ import annotations

import streamlit as st

from backend.services.auth_service import create_user, get_user, login_user


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

    if sign_up:
        try:
            create_user(email=email, password=password)
        except ValueError as exc:
            st.sidebar.error(str(exc))
            return
        st.session_state["signed_in"] = True
        st.session_state["user_email"] = email
        user = get_user(email)
        st.session_state["user_name"] = user.get("name", email.split("@")[0])
        st.session_state["show_genre_onboarding"] = True
        st.sidebar.success("Account created and signed in.")
        st.rerun()

    try:
        record = login_user(email=email, password=password)
    except ValueError as exc:
        st.sidebar.error(str(exc))
        return

    st.session_state["signed_in"] = True
    st.session_state["user_email"] = email
    st.session_state["user_name"] = record.get("name", email.split("@")[0])
    st.sidebar.success("Signed in.")
    st.rerun()

