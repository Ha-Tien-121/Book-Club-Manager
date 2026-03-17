"Helper functions for formatting title data in the SPL and Amazon datasets."


import re

import pandas as pd


def format_title(titles):
    """
    Formats title data (string or pandas Series) by:
    (1) removing quotes
    (2) removing slash and following text
    (3) removing colon and following text
    (4) removing semicolon and following text
    (5) removing text in parentheses (e.g., "Title (2nd edition)")
    (6) removing edition info after comma (e.g., "Title, 2nd edition")
    (7) stripping whitespace on edges
    (8) normalizing multiple spaces to a single space
    (9) removing trailing punctuation like periods and commas
    
    Args:
        title (str or pandas.Series): input title or series of titles
    
    Returns:
        str or pandas.Series: formatted title in the same type as input
    """
    def _format_single_title(title):
        """Format one title string using the shared normalization rules."""
        title = str(title)
        title = title.replace('"', '')
        title = re.sub(r'/.*', '', title)
        title = re.sub(r':.*', '', title)
        title = re.sub(r';.*', '', title)
        title = re.sub(r'\([^)]*\)', '', title)
        title = re.sub(r',?\s*\d+(st|nd|rd|th)\s*edition', '', title, flags=re.I)
        title = title.strip()
        title = re.sub(r'\s+', ' ', title)
        title = re.sub(r'[.,]$', '', title)
        return title
    if not isinstance(titles, (pd.Series, str)):
        return None
    if isinstance(titles, pd.Series):
        return titles.apply(_format_single_title)
    return _format_single_title(titles)
