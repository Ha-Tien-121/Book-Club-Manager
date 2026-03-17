"""Bookish Streamlit entrypoint."""

from frontend.main import main

main()
# Entrypoint intentionally keeps side effects at import time.
APP_ENTRYPOINT_LOADED = True
