import json
import csv

input_file = "meta_Books.jsonl"
output_file = "meta_Books_cleaned.csv"

columns_to_remove = ['main_category', 'features', 'price', 'videos', 'store', 'details', 'bought_together', 'subtitle']

# Taken from top 20 most common categories, covering about 70% of books in dataset. 
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

        book['categories'] = [cat for cat in book['categories'] if cat in top_categories]

        for col in columns_to_remove:
            book.pop(col, None)
        if writer is None:
            headers = list(book.keys())
            writer = csv.DictWriter(outfile, fieldnames=headers)
            writer.writeheader()
        
        writer.writerow(book)