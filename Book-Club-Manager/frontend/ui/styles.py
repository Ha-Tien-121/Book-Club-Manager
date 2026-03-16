"""UI styling helpers for Streamlit."""

from __future__ import annotations

import streamlit as st


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
