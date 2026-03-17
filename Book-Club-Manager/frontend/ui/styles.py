"""UI styling helpers for Streamlit.

Theme: Bebas Neue + Lato, palette #252422, #f1e6e4, #ef98a8, #d37689,
#d78f67, #b87048, #d9624c, #ffd84d, #274594.
"""

from __future__ import annotations

import streamlit as st

# Theme colors from presentation
_THEME = {
    "dark": "#252422",
    "cream": "#f1e6e4",
    "pink": "#ef98a8",
    "dusty_rose": "#d37689",
    "terracotta": "#d78f67",
    "brown": "#b87048",
    "coral": "#d9624c",
    "yellow": "#ffd84d",
    "blue": "#274594",
}


def inject_styles() -> None:
    """Inject custom CSS: Bebas Neue + Lato, presentation color palette."""
    st.markdown(
        """
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Lato:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">
        <style>
        :root {
            --theme-dark: #252422;
            --theme-cream: #f1e6e4;
            --theme-pink: #ef98a8;
            --theme-dusty-rose: #d37689;
            --theme-terracotta: #d78f67;
            --theme-brown: #b87048;
            --theme-coral: #d9624c;
            --theme-yellow: #ffd84d;
            --theme-blue: #274594;
            --theme-gray: #d3d3d3;         /* neutral gray for borders */
            --theme-gray-warm: #e3d9d7;    /* warmer gray for hovers/fills */
        }
        /* Base and layout */
        .stApp, [data-testid="stAppViewContainer"], .main .block-container {
            background-color: var(--theme-cream);
            font-family: 'Lato', sans-serif;
            color: var(--theme-dark);
        }
        .block-container { padding-top: 3.75rem; max-width: 1100px; }
        /* Headings: Bebas Neue */
        h1, h2, h3, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
            font-family: 'Bebas Neue', sans-serif !important;
            color: var(--theme-dark) !important;
            letter-spacing: 0.02em;
        }
        .stMarkdown p, .stMarkdown span, .stCaption {
            color: var(--theme-dark);
            font-family: 'Lato', sans-serif;
        }
        /* Top header / toolbar (Deploy, etc.) – match cream so it doesn't stick out */
        header[data-testid="stHeader"],
        .stApp > header,
        div[data-testid="stHeader"],
        section[data-testid="stHeader"],
        [data-testid="stToolbar"],
        [data-testid="stDecoration"] {
            background: var(--theme-cream) !important;
        }
        .stApp header,
        [data-testid="stHeader"] {
            border-bottom: 2px solid var(--theme-terracotta) !important;
        }
        header button, header a, [data-testid="stToolbar"] button, [data-testid="stToolbar"] a {
            color: var(--theme-dark) !important;
        }
        /* Sidebar: terracotta tint + off-white gradient */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #f0e4df 0%, var(--theme-cream) 100%) !important;
            font-family: 'Lato', sans-serif;
            border-right: 3px solid var(--theme-terracotta);
        }
        [data-testid="stSidebar"] .stMarkdown,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span,
        [data-testid="stSidebar"] label {
            color: var(--theme-dark) !important;
        }
        /* Sidebar title "Bookish": bigger + terracotta */
        [data-testid="stSidebar"] h1 {
            color: var(--theme-terracotta) !important;
            font-size: 2.25rem !important;
            letter-spacing: 0.04em;
        }
        /* Sidebar buttons (Create account, Sign in): #b87048 (brown) with white text */
        [data-testid="stSidebar"] .stButton > button,
        [data-testid="stSidebar"] form .stButton > button,
        [data-testid="stSidebar"] form button[type="submit"],
        [data-testid="stSidebar"] button[kind="primary"],
        [data-testid="stSidebar"] button[kind="secondary"],
        [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] > button,
        [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] button {
            background-color: var(--theme-brown) !important;
            color: #ffffff !important;
        }
        /* Override Streamlit secondary/default button style so Sign in is not white */
        [data-testid="stSidebar"] form [data-baseweb="button"] {
            background-color: var(--theme-brown) !important;
            color: #ffffff !important;
        }
        /* White text for Sign in and all sidebar button labels (including form submit) */
        [data-testid="stSidebar"] .stButton > button *,
        [data-testid="stSidebar"] form .stButton > button *,
        [data-testid="stSidebar"] form [data-baseweb="button"] *,
        [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] button *,
        [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] * {
            color: #ffffff !important;
        }
        [data-testid="stSidebar"] .stButton > button:hover,
        [data-testid="stSidebar"] form .stButton > button:hover {
            background-color: var(--theme-gray) !important;
            color: var(--theme-dark) !important;
        }
        /* Sign-in inputs and filter/select bars: white */
        [data-testid="stSidebar"] input,
        [data-testid="stSidebar"] [data-baseweb="select"] > div,
        [data-testid="stSidebar"] .stSelectbox > div {
            background-color: white !important;
            border-color: #b8b8b8 !important;
        }
        /* Tabs: terracotta underline */
        .stTabs [data-baseweb="tab-list"] {
            margin-top: 0.5rem;
            position: relative;
            z-index: 2;
            background: transparent;
            border-bottom: 2px solid var(--theme-terracotta);
        }
        .stTabs [data-baseweb="tab"] {
            font-family: 'Bebas Neue', sans-serif !important;
            font-size: 1.1rem;
            letter-spacing: 0.05em;
            color: var(--theme-dark);
        }
        .stTabs [aria-selected="true"] {
            color: var(--theme-blue) !important;
        }
        /* Pills: genre tags – warm grey so they don't compete with alerts/buttons */
        .pill {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            border: 1px solid var(--theme-gray);
            background: var(--theme-gray-warm);
            color: #555555;
            font-family: 'Lato', sans-serif;
            font-size: 0.75rem;
            margin-right: 0.35rem;
            margin-top: 0.25rem;
        }
        /* Buttons: blue with white text; hover = warm grey */
        .stButton > button,
        button[kind="primary"],
        button[kind="secondary"] {
            font-family: 'Lato', sans-serif !important;
            background-color: var(--theme-blue) !important;
            color: #ffffff !important;
            border: none;
            border-radius: 6px;
        }
        .stButton > button * {
            color: #ffffff !important;
        }
        .stButton > button:hover,
        .stButton > button:hover *,
        button[kind="primary"]:hover,
        button[kind="secondary"]:hover {
            background-color: var(--theme-gray-warm) !important;
            color: var(--theme-dark) !important;
        }
        .stButton > button:hover * {
            color: var(--theme-dark) !important;
        }
        /* Links */
        a { color: var(--theme-blue); }
        a:hover { color: var(--theme-coral); }
        /* Inputs and selectboxes (filter bars, sign-in): white */
        .stTextInput input,
        .stSelectbox > div,
        [data-baseweb="select"] > div {
            background-color: white !important;
            border-color: #b8b8b8 !important;
            font-family: 'Lato', sans-serif;
        }
        /* Cards: off-white with terracotta border */
        [data-testid="stVerticalBlock"] > div[style*="border"] {
            border-color: var(--theme-terracotta) !important;
            background: var(--theme-cream);
        }
        /* Success / Saved in main content: light dusty rose tint from scheme */
        [data-testid="stAlert"],
        .stAlert,
        div[data-testid="stAlert"] > div {
            background-color: #f5e8e6 !important;
            border: 1px solid var(--theme-dusty-rose) !important;
            border-left: 4px solid var(--theme-dusty-rose) !important;
            color: var(--theme-dark) !important;
        }
        [data-testid="stAlert"] *,
        .stAlert * {
            color: var(--theme-dark) !important;
        }
        /* Sidebar welcome / signed-in messages: softer, more welcoming than Saved */
        [data-testid="stSidebar"] [data-testid="stAlert"],
        [data-testid="stSidebar"] .stAlert,
        [data-testid="stSidebar"] div[data-testid="stAlert"] > div {
            background-color: #fdf4f1 !important;  /* very light warm tint */
            border-radius: 12px !important;
            border: none !important;
            box-shadow: 0 0 0 1px rgba(215,118,137,0.25);
            padding: 0.6rem 0.9rem !important;
        }
        [data-testid="stSidebar"] [data-testid="stAlert"] *,
        [data-testid="stSidebar"] .stAlert * {
            color: var(--theme-dark) !important;
            font-weight: 500;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
