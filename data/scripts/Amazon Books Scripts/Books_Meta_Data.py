"""
Cleans the Amazon books meta data by 
(1) removing unnecessary columns
(2) only keeping subset of 20 most popular categories (genres)
(3) takes only the author name from "Author" column
(4) takes only the large image url from "images" column.

Args:
    meta_Books.jsonl: The input dataset of Amazon books metadata in JSON Lines format.

Returns:
    meta_books_cleaned.csv: The cleaned dataset as csv. 
    Columns: title (str), average_rating (float), rating_number (int), description (list of str), 
    images (str: url), categories (list of str), parent_asin (int: 10 digits), 
    author_name (str: "first last")
"""


import json
import csv

input_file = "meta_Books.jsonl"
output_file = "meta_books_cleaned.csv"

columns_to_remove = ['main_category', 'features', 'price', 'videos', 'store', 'details', 'bought_together', 'subtitle']

# Taken from top 20 most common categories, covering about 70% of books in dataset. 
# LGTQ+ tag
top_categories = {
    'Literature & Fiction', "Children's Books", 'Genre Fiction', 
    'Mystery, Thriller & Suspense', 'Arts & Photography', 'History', 
    'Biographies & Memoirs', 'Science Fiction & Fantasy', 
    'Crafts, Hobbies & Home', 'Christian Books & Bibles', 'Thrillers & Suspense', 
    'Business & Money', 'Politics & Social Sciences', 
    'Growing Up & Facts of Life', 'Romance', 'Science & Math', 'Teen & Young Adult'
}

with open(input_file, 'r') as fp, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = None
    
    for line in fp:
        book = json.loads(line)

        if isinstance(book.get("author"), dict) and book["author"].get("name"):
             book["author_name"] = book["author"].get("name")
        else:
             book["author_name"] = None
        book.pop("author", None)

        if isinstance(book.get("images"), list) and len(book["images"]) > 0 and isinstance(book["images"][0], dict) and book["images"][0].get("large"):
            book["images"] = book["images"][0].get("large")
        else:
            book["images"] = None
        
        book['categories'] = [cat for cat in book['categories'] if cat in top_categories]

        for col in columns_to_remove:
            book.pop(col, None)
        if writer is None:
            headers = list(book.keys())
            writer = csv.DictWriter(outfile, fieldnames=headers)
            writer.writeheader()
        
        writer.writerow(book)