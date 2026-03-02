"Helper functions for cleaning text data in the SPL datasets."
def clean_text(series):
    """
    Cleans a pandas Series of text by:
    (1) removing quotes
    (2) removing slash and following text (SPL titles often have format "Title / extra info")
    (3) lowercasing
    (4) trimming whitespace on edges
    (5) normalizing multiple spaces to a single space
    """
    return (
        series
        .astype(str)
        .str.replace('"', '', regex=False)
        .str.replace(r'/.*', '', regex=True)
        .str.lower()
        .str.strip()
        .str.replace(r'\s+', ' ', regex=True)
    )
