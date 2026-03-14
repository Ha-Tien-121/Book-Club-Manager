"""Skeleton book recommender: returns top 50 books (first 50 from sample data)."""

import json

# First 50 books from books_sample_100.json (legit data, manually embedded, no description)
_TOP_50_JSON = """
[
  {
    "parent_asin": "0701169850",
    "title": "Chaucer",
    "author_name": "Peter Ackroyd",
    "average_rating": 4.5,
    "rating_number": 29,
    "images": "https://m.media-amazon.com/images/I/41X61VPJYKL._SX334_BO1,204,203,200_.jpg",
    "categories": "[\\"Literature & Fiction\\"]",
    "title_author_key": "chaucer|peter ackroyd"
  },
  {
    "parent_asin": "0435088688",
    "title": "Notes from a Kidwatcher",
    "author_name": "Yetta M. Goodman",
    "average_rating": 5.0,
    "rating_number": 1,
    "images": "https://m.media-amazon.com/images/I/41bfTRxpMML._SX218_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[]",
    "title_author_key": "notes from a kidwatcher|yetta m goodman"
  },
  {
    "parent_asin": "0316185361",
    "title": "Service",
    "author_name": "Marcus Luttrell",
    "average_rating": 4.7,
    "rating_number": 3421,
    "images": "https://m.media-amazon.com/images/I/41YQHDWRyGL._SX321_BO1,204,203,200_.jpg",
    "categories": "[\\"Biographies & Memoirs\\"]",
    "title_author_key": "service|marcus luttrell"
  },
  {
    "parent_asin": "0545425573",
    "title": "Monstrous Stories #4",
    "author_name": null,
    "average_rating": 4.4,
    "rating_number": 40,
    "images": "https://m.media-amazon.com/images/I/614Mx0QCe7L._SX339_BO1,204,203,200_.jpg",
    "categories": "[\\"Children's Books\\"]",
    "title_author_key": null
  },
  {
    "parent_asin": "B00KFOP3RG",
    "title": "Parker & Knight",
    "author_name": "Donald Wells",
    "average_rating": 4.5,
    "rating_number": 381,
    "images": "https://m.media-amazon.com/images/I/41j6GpAqFBL.jpg",
    "categories": "[\\"Mystery, Thriller & Suspense\\"]",
    "title_author_key": "parker & knight|donald wells"
  },
  {
    "parent_asin": "B09PHG4FQ8",
    "title": "Writings from a Black Woman Living in the Land of the Free",
    "author_name": null,
    "average_rating": 5.0,
    "rating_number": 5,
    "images": "https://m.media-amazon.com/images/I/417CItk7HML._SX387_BO1,204,203,200_.jpg",
    "categories": "[\\"Arts & Photography\\"]",
    "title_author_key": null
  },
  {
    "parent_asin": "B0086HQWC4",
    "title": "Child Development",
    "author_name": null,
    "average_rating": 5.0,
    "rating_number": 2,
    "images": "https://m.media-amazon.com/images/I/41b2UvkHaAL._SX331_BO1,204,203,200_.jpg",
    "categories": "[]",
    "title_author_key": null
  },
  {
    "parent_asin": "1680450263",
    "title": "Make",
    "author_name": "Charles Platt",
    "average_rating": 4.7,
    "rating_number": 1366,
    "images": "https://m.media-amazon.com/images/I/51j48HH1P9L._SX218_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[]",
    "title_author_key": "make|charles platt"
  },
  {
    "parent_asin": "1694621731",
    "title": "Reunion",
    "author_name": null,
    "average_rating": 4.9,
    "rating_number": 12,
    "images": "https://m.media-amazon.com/images/I/313xN7wqDQL._SX331_BO1,204,203,200_.jpg",
    "categories": "[\\"Literature & Fiction\\"]",
    "title_author_key": null
  },
  {
    "parent_asin": "1932225323",
    "title": "Four Centuries of American Education",
    "author_name": "David Barton",
    "average_rating": 4.8,
    "rating_number": 133,
    "images": "https://m.media-amazon.com/images/I/415r5RJ7alL._SY291_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[]",
    "title_author_key": "four centuries of american education|david barton"
  },
  {
    "parent_asin": "0893011673",
    "title": "Mining Engineers and the American West",
    "author_name": "Clark C. Spence",
    "average_rating": 4.7,
    "rating_number": 4,
    "images": "https://m.media-amazon.com/images/I/51nlRLa5ycL._SX334_BO1,204,203,200_.jpg",
    "categories": "[\\"History\\"]",
    "title_author_key": "mining engineers and the american west|clark c spence"
  },
  {
    "parent_asin": "9083256898",
    "title": "Heart of Silk and Shadows",
    "author_name": "Lisette Marshall",
    "average_rating": 4.4,
    "rating_number": 481,
    "images": "https://m.media-amazon.com/images/I/41tMRHMU2WL._SY291_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[\\"Fantasy\\"]",
    "title_author_key": "heart of silk and shadows|lisette marshall"
  },
  {
    "parent_asin": "1771682760",
    "title": "Girl Made of Glass",
    "author_name": "Shelby Leigh",
    "average_rating": 4.5,
    "rating_number": 117,
    "images": "https://m.media-amazon.com/images/I/41-jq+4xBwL._SY344_BO1,204,203,200_.jpg",
    "categories": "[\\"Literature & Fiction\\", \\"Poetry\\"]",
    "title_author_key": "girl made of glass|shelby leigh"
  },
  {
    "parent_asin": "1087848539",
    "title": "The Old Man and the Pirate Princess",
    "author_name": "Jessica Mathews",
    "average_rating": 3.2,
    "rating_number": 5,
    "images": "https://m.media-amazon.com/images/I/41FXXfN-fYL._SX384_BO1,204,203,200_.jpg",
    "categories": "[\\"Children's Books\\"]",
    "title_author_key": "the old man and the pirate princess|jessica mathews"
  },
  {
    "parent_asin": "0710306911",
    "title": "Japanese Girls and Women",
    "author_name": "Alice Mabel Bacon",
    "average_rating": 3.2,
    "rating_number": 7,
    "images": "https://m.media-amazon.com/images/I/21BhwngmjwL._SX317_BO1,204,203,200_.jpg",
    "categories": "[\\"History\\"]",
    "title_author_key": "japanese girls and women|alice mabel bacon"
  },
  {
    "parent_asin": "0130840963",
    "title": "Behavior Principles in Everyday Life",
    "author_name": "John D. Baldwin",
    "average_rating": 3.7,
    "rating_number": 14,
    "images": "https://m.media-amazon.com/images/I/41BZJS25WAL._SX218_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[]",
    "title_author_key": "behavior principles in everyday life|john d baldwin"
  },
  {
    "parent_asin": "8477110190",
    "title": "PQL 3 - Lola",
    "author_name": null,
    "average_rating": 3.7,
    "rating_number": 2,
    "images": "https://m.media-amazon.com/images/I/51+wU+PzoWL._SX338_BO1,204,203,200_.jpg",
    "categories": "[]",
    "title_author_key": null
  },
  {
    "parent_asin": "1275627234",
    "title": "A sermon, preached at the execution of Moses Paul, an Indian",
    "author_name": null,
    "average_rating": 3.8,
    "rating_number": 2,
    "images": "https://m.media-amazon.com/images/I/51pcm5alT8L._SX382_BO1,204,203,200_.jpg",
    "categories": "[\\"History\\"]",
    "title_author_key": null
  },
  {
    "parent_asin": "1609303687",
    "title": "Business Associations",
    "author_name": "William A. Klein",
    "average_rating": 4.5,
    "rating_number": 6,
    "images": "https://m.media-amazon.com/images/I/41vFoSjfSQL._SX352_BO1,204,203,200_.jpg",
    "categories": "[]",
    "title_author_key": "business associations|william a klein"
  },
  {
    "parent_asin": "B003P2WETK",
    "title": "Inspector Imanishi Investigates (Soho Crime)",
    "author_name": "Seicho Matsumoto",
    "average_rating": 4.0,
    "rating_number": 1138,
    "images": "https://m.media-amazon.com/images/I/51UHERs91bL.jpg",
    "categories": "[\\"Mystery, Thriller & Suspense\\"]",
    "title_author_key": "inspector imanishi investigates (soho crime)|seicho matsumoto"
  },
  {
    "parent_asin": "B000JDT7L6",
    "title": "Officially Dead",
    "author_name": "Quentin Reynolds",
    "average_rating": 4.6,
    "rating_number": 3,
    "images": "https://m.media-amazon.com/images/I/41YJsa+LdKL._SX373_BO1,204,203,200_.jpg",
    "categories": "[]",
    "title_author_key": "officially dead|quentin reynolds"
  },
  {
    "parent_asin": "0974028622",
    "title": "Dine In!",
    "author_name": "Nick Stellino",
    "average_rating": 4.5,
    "rating_number": 27,
    "images": "https://m.media-amazon.com/images/I/51G21WXBNYL._SX218_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[\\"Cookbooks, Food & Wine\\"]",
    "title_author_key": "dine in!|nick stellino"
  },
  {
    "parent_asin": "B098JWSP78",
    "title": "The Promise of Love (The Book of Love)",
    "author_name": "Meara Platt",
    "average_rating": 4.5,
    "rating_number": 637,
    "images": "https://m.media-amazon.com/images/I/51vzMDej+IS._SY344_BO1,204,203,200_.jpg",
    "categories": "[\\"Romance\\"]",
    "title_author_key": "the promise of love (the book of love)|meara platt"
  },
  {
    "parent_asin": "B0006AOORE",
    "title": "Statesmen of the Lost Cause",
    "author_name": null,
    "average_rating": 5.0,
    "rating_number": 2,
    "images": "https://m.media-amazon.com/images/I/51DwdP1FEiL._SX336_BO1,204,203,200_.jpg",
    "categories": "[\\"History\\"]",
    "title_author_key": null
  },
  {
    "parent_asin": "0780274164",
    "title": "A Monster Sandwich (The Story Box - Level 1 - Set B - for Emergent Readers) [Paperback]",
    "author_name": "Joy Cowley",
    "average_rating": 3.8,
    "rating_number": 5,
    "images": "https://m.media-amazon.com/images/I/51Al3nLgqhL._SX218_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[]",
    "title_author_key": "a monster sandwich (the story box - level 1 - set b - for emergent readers) [paperback]|joy cowley"
  },
  {
    "parent_asin": "0435088432",
    "title": "Sounds from the Heart",
    "author_name": "Maureen Barbieri",
    "average_rating": 5.0,
    "rating_number": 2,
    "images": "https://m.media-amazon.com/images/I/41SM7VFW35L._SX301_BO1,204,203,200_.jpg",
    "categories": "[]",
    "title_author_key": "sounds from the heart|maureen barbieri"
  },
  {
    "parent_asin": "0553819399",
    "title": "Hunting Evil",
    "author_name": "Guy Walters",
    "average_rating": 4.2,
    "rating_number": 634,
    "images": "https://m.media-amazon.com/images/I/51o89h0rrsL._SX319_BO1,204,203,200_.jpg",
    "categories": "[\\"History\\"]",
    "title_author_key": "hunting evil|guy walters"
  },
  {
    "parent_asin": "0500016550",
    "title": "Les Chiens De Paris",
    "author_name": "Barnaby Conrad III",
    "average_rating": 4.6,
    "rating_number": 8,
    "images": "https://m.media-amazon.com/images/I/51xoVQCwaBL._SX488_BO1,204,203,200_.jpg",
    "categories": "[\\"Crafts, Hobbies & Home\\"]",
    "title_author_key": "les chiens de paris|barnaby conrad iii"
  },
  {
    "parent_asin": "1929145667",
    "title": "Acoustic & Digital Piano Buyer Fall 2017",
    "author_name": null,
    "average_rating": 5.0,
    "rating_number": 3,
    "images": "https://m.media-amazon.com/images/I/518HbE+JNhL._SX378_BO1,204,203,200_.jpg",
    "categories": "[\\"Arts & Photography\\"]",
    "title_author_key": null
  },
  {
    "parent_asin": "1536832286",
    "title": "Sugary Sweets (A Taste of Love Series)",
    "author_name": "A.M. Willard",
    "average_rating": 4.4,
    "rating_number": 119,
    "images": "https://m.media-amazon.com/images/I/51yHh0rQJvL._SX322_BO1,204,203,200_.jpg",
    "categories": "[\\"Literature & Fiction\\"]",
    "title_author_key": "sugary sweets (a taste of love series)|am willard"
  },
  {
    "parent_asin": "B005GG0N1E",
    "title": "The English Monster",
    "author_name": "Lloyd Shepherd",
    "average_rating": 4.0,
    "rating_number": 177,
    "images": "https://m.media-amazon.com/images/I/51JlBHsl0ML.jpg",
    "categories": "[\\"Mystery, Thriller & Suspense\\"]",
    "title_author_key": "the english monster|lloyd shepherd"
  },
  {
    "parent_asin": "1852691247",
    "title": "The Very Hungry Caterpillar",
    "author_name": "Eric Carle",
    "average_rating": 4.6,
    "rating_number": 294,
    "images": "https://m.media-amazon.com/images/I/41LCW+nsJ6S._SY340_BO1,204,203,200_.jpg",
    "categories": "[\\"Children's Books\\"]",
    "title_author_key": "the very hungry caterpillar|eric carle"
  },
  {
    "parent_asin": "1530178770",
    "title": "More Together",
    "author_name": null,
    "average_rating": 5.0,
    "rating_number": 1,
    "images": "https://m.media-amazon.com/images/I/61MKCqzu9rL._SX404_BO1,204,203,200_.jpg",
    "categories": "[]",
    "title_author_key": null
  },
  {
    "parent_asin": "0805005277",
    "title": "Never Sniff a Gift Fish",
    "author_name": "Patrick J. McManus",
    "average_rating": 4.8,
    "rating_number": 413,
    "images": "https://m.media-amazon.com/images/I/51d+y3WBQgL._SX340_BO1,204,203,200_.jpg",
    "categories": "[]",
    "title_author_key": "never sniff a gift fish|patrick j mcmanus"
  },
  {
    "parent_asin": "1584885874",
    "title": "Markov Chain Monte Carlo",
    "author_name": "Dani Gamerman",
    "average_rating": 4.4,
    "rating_number": 19,
    "images": "https://m.media-amazon.com/images/I/41wmzCw+buL._SX360_BO1,204,203,200_.jpg",
    "categories": "[\\"Science & Math\\"]",
    "title_author_key": "markov chain monte carlo|dani gamerman"
  },
  {
    "parent_asin": "194557299X",
    "title": "Therapy Mammals",
    "author_name": "Jon Methven",
    "average_rating": 4.2,
    "rating_number": 24,
    "images": "https://m.media-amazon.com/images/I/41quRCEOHmL._SX323_BO1,204,203,200_.jpg",
    "categories": "[\\"Literature & Fiction\\"]",
    "title_author_key": "therapy mammals|jon methven"
  },
  {
    "parent_asin": "0393062651",
    "title": "A Most Dangerous Book",
    "author_name": "Christopher B. Krebs",
    "average_rating": 4.2,
    "rating_number": 110,
    "images": "https://m.media-amazon.com/images/I/41bnYeddqPL._SY291_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[\\"Literature & Fiction\\"]",
    "title_author_key": "a most dangerous book|christopher b krebs"
  },
  {
    "parent_asin": "1947844938",
    "title": "The Prophet",
    "author_name": "Kahlil Gibran",
    "average_rating": 4.6,
    "rating_number": 1367,
    "images": "https://m.media-amazon.com/images/I/41OD1btm2EL._SY291_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[\\"Literature & Fiction\\", \\"Poetry\\"]",
    "title_author_key": "the prophet|kahlil gibran"
  },
  {
    "parent_asin": "B0BW2GGCNZ",
    "title": "The Melancholy Strumpet Master",
    "author_name": "Zeb Beck",
    "average_rating": 4.6,
    "rating_number": 14,
    "images": "https://m.media-amazon.com/images/I/41jfndqfi+L._SX322_BO1,204,203,200_.jpg",
    "categories": "[\\"Literature & Fiction\\"]",
    "title_author_key": "the melancholy strumpet master|zeb beck"
  },
  {
    "parent_asin": "0531253090",
    "title": "Egypt (Enchantment of the World)",
    "author_name": "Ann Heinrichs",
    "average_rating": 5.0,
    "rating_number": 3,
    "images": "https://m.media-amazon.com/images/I/51fVIKoAvEL._SX429_BO1,204,203,200_.jpg",
    "categories": "[\\"Children's Books\\"]",
    "title_author_key": "egypt (enchantment of the world)|ann heinrichs"
  },
  {
    "parent_asin": "1916260101",
    "title": "Shane, Sheba and Sky",
    "author_name": "Paul Viner",
    "average_rating": 4.9,
    "rating_number": 75,
    "images": "https://m.media-amazon.com/images/I/41W+Wxi5W0L._SX322_BO1,204,203,200_.jpg",
    "categories": "[\\"Biographies & Memoirs\\"]",
    "title_author_key": "shane, sheba and sky|paul viner"
  },
  {
    "parent_asin": "0674975812",
    "title": "Law and Legitimacy in the Supreme Court",
    "author_name": "Richard H. Fallon",
    "average_rating": 4.7,
    "rating_number": 8,
    "images": "https://m.media-amazon.com/images/I/413FhkTEGiL._SY291_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[\\"Politics & Social Sciences\\"]",
    "title_author_key": "law and legitimacy in the supreme court|richard h fallon"
  },
  {
    "parent_asin": "B014X8T69U",
    "title": "Retreat Yourself",
    "author_name": null,
    "average_rating": 3.8,
    "rating_number": 4,
    "images": "https://m.media-amazon.com/images/I/51BTXHgM6lL.jpg",
    "categories": "[]",
    "title_author_key": null
  },
  {
    "parent_asin": "B07M6RG1RN",
    "title": "Reptilian",
    "author_name": "John J. Rust",
    "average_rating": 4.1,
    "rating_number": 162,
    "images": "https://m.media-amazon.com/images/I/512aqGZvOnL.jpg",
    "categories": "[\\"Literature & Fiction\\"]",
    "title_author_key": "reptilian|john j rust"
  },
  {
    "parent_asin": "1530154685",
    "title": "Grade Five Music Theory",
    "author_name": "Victoria Williams",
    "average_rating": 4.6,
    "rating_number": 87,
    "images": "https://m.media-amazon.com/images/I/51RnXmZTT9L._SX218_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[\\"Arts & Photography\\"]",
    "title_author_key": "grade five music theory|victoria williams"
  },
  {
    "parent_asin": "158355128X",
    "title": "Oregon Birds",
    "author_name": "James Kavanagh",
    "average_rating": 4.8,
    "rating_number": 74,
    "images": "https://m.media-amazon.com/images/I/51-Hx-hI-ML._SY291_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[\\"Science & Math\\"]",
    "title_author_key": "oregon birds|james kavanagh"
  },
  {
    "parent_asin": "B0006ENZC0",
    "title": "Who'd a thought it!",
    "author_name": null,
    "average_rating": 5.0,
    "rating_number": 1,
    "images": "https://m.media-amazon.com/images/I/51d+WkHZvrL._SX309_BO1,204,203,200_.jpg",
    "categories": "[]",
    "title_author_key": null
  },
  {
    "parent_asin": "0062279068",
    "title": "Clark the Shark",
    "author_name": "Bruce Hale",
    "average_rating": 4.8,
    "rating_number": 447,
    "images": "https://m.media-amazon.com/images/I/51nRw6MzjbL._SY291_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[\\"Children's Books\\", \\"Growing Up & Facts of Life\\"]",
    "title_author_key": "clark the shark|bruce hale"
  },
  {
    "parent_asin": "1940250064",
    "title": "Primeval",
    "author_name": "Blood Bound Books",
    "average_rating": 3.3,
    "rating_number": 10,
    "images": "https://m.media-amazon.com/images/I/51wG04LErhL._SX322_BO1,204,203,200_.jpg",
    "categories": "[\\"Literature & Fiction\\"]",
    "title_author_key": "primeval|blood bound books"
  },
  {
    "parent_asin": "1093861355",
    "title": "The Mercenary Code (The Shattering of Kingdoms)",
    "author_name": "Emmet Moss",
    "average_rating": 4.4,
    "rating_number": 521,
    "images": "https://m.media-amazon.com/images/I/51rgTdj3XQL._SY291_BO1,204,203,200_QL40_FMwebp_.jpg",
    "categories": "[\\"Fantasy\\"]",
    "title_author_key": "the mercenary code (the shattering of kingdoms)|emmet moss"
  }
]
"""


def recommend_for_user(user_email: str) -> list[dict]:
    """Return the top N recommended books. Skeleton: returns first 50 from embedded data."""
    books = json.loads(_TOP_50_JSON)
    return books
