"""Sidebar auth UI: Sign in and link to Create account page."""

from __future__ import annotations

import streamlit as st

from backend.services.auth_service import get_user, login_user


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

