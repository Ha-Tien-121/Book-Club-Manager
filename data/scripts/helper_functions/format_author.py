"""Helper functions for formatting author name in the SPL and Amazon datasets."""

import pandas as pd
import re

def format_author(authors):
    """
    Format author names to be more readable.

    Formats author names by:
    (1) removing parenthetical info (eg. "Smith, John (Editor)" -> "Smith, John")
    (2) stripping extra whitespace
    (3) keeping only the first two comma-separated elements
    (4) reversing "last, first" to "first last"

    Args:
        author (str, pd.Series): author name string "last, first, ..." or "first last"

    Returns:
        str or pandas.Series: formatted author name (first last) in the same type as input
    """

    def _format_single_author(author):
        author = str(author)
        author = re.sub(r'\(.*?\)', '', str(author)) 
        parts = [" ".join(p.split()) for p in author.split(",")]
        if len(parts) >= 2:
            return f"{parts[1]} {parts[0]}"
        return parts[0]

    if not isinstance(authors, (pd.Series, str)):
        return None
    if isinstance(authors, pd.Series):
        return authors.apply(_format_single_author)
    return _format_single_author(authors)
