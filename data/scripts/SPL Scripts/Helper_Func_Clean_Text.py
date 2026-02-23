def clean_text(series):
    return (
        series
        .astype(str)
        .str.replace('"', '', regex=False)        # remove quotes
        .str.replace(r'/.*', '', regex=True)     # remove slash and after
        .str.lower()                             # lowercase
        .str.strip()                             # trim edges
        .str.replace(r'\s+', ' ', regex=True)    # normalize whitespace
    )