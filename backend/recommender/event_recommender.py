"""Skeleton event recommender: returns top 10 events (first 10 from bookclubs_seattle_clean.json)."""

import json

# First 10 events from bookclubs_seattle_clean.json (legit data, manually embedded)
_TOP_10_JSON = """
[
  {
    "event_id": "e843389a1443deed",
    "title": "Tool Library Book Club",
    "link": "https://seattlereconomy.org/event/tool-library-book-club-9/",
    "description": "We will discuss \\"Hollow Kingdom\\" by Kira Jane Buxton. Everyone is welcome!Join us for the tenth edition of the NE Seattle Tool Library\\\\'s book club! We’ll be reading the Hollow […]\\\\n",
    "book_title": "Hollow Kingdom",
    "book_author": "Kira Jane Buxton",
    "tags": [],
    "start_iso": "2026-02-18T19:00:00",
    "start_time": "7:00 PM",
    "day_of_week_start": "Wed",
    "city_state": "Seattle, WA",
    "thumbnail": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcROVRRjC_JWs2--u0yHgc2jJO2ZoKGx2rcFRZgOJ6VeYszvkVFMcqYi6qc&s"
  },
  {
    "event_id": "043aafa16c4ee86f",
    "title": "Bookish Trivia Night",
    "link": "https://theticket.seattletimes.com/calendar/?_evDiscoveryPath=/event/3430587-romance-trivia-with-silent-book-club-capitol-hill-and-freeze-tag",
    "description": "Join Silent Book Club Capitol Hill and Freeze Tag for a post Valentine’s themed bookish trivia event ! 🤔 Romance Based Trivia - Find out how much you know about romantic and platonic...",
    "book_title": "",
    "book_author": "",
    "tags": [
      "Romance"
    ],
    "start_iso": "2026-02-19T18:30:00",
    "start_time": "6:30 PM",
    "day_of_week_start": "Thu",
    "city_state": "Seattle, WA",
    "thumbnail": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTuUQnD57ihAiQm17z5mW3YcX73NiKBVeFyQL-NXkcJcP366iPlG6fWfww&s"
  },
  {
    "event_id": "e267fb651db5550e",
    "title": "SJCC Book Club",
    "link": "https://sjcc.org/event/sjcc-book-club/",
    "description": "Join us as we launch a new SJCC Book Club, a space for thoughtful reading, meaningful discussion, and connection across our SJCC community. Our first selection is “Hostage,” by Eli Sharabi, a...",
    "book_title": "Hostage",
    "book_author": "Eli Sharabi",
    "tags": [],
    "start_iso": "2026-02-17T18:30:00",
    "start_time": "6:30 PM",
    "day_of_week_start": "Tue",
    "city_state": "Mercer Island, WA",
    "thumbnail": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRVSlsImCm5Wd9ysMvYcRk2gAfw3NWCOV7dhjGihEHGgtAjrqZplv-k85Y&s"
  },
  {
    "event_id": "f0e42581af22ead6",
    "title": "Asylum Book Club",
    "link": "https://www.eventbrite.com/e/silent-reading-jazz-wine-bar-coffee-tickets-1956762151269?aff=erelexpmlt",
    "description": "Unwind with a book, global jazz, and bottomless drinks in hand. BYOB (Bring Your Own Book) or borrow one from Asylum's library. 21+ Silent Reading Night @ Asylum | 7PM–9PM 📍 Asylum Collective ...",
    "book_title": "Night @ Asylum | 7PM–9PM 📍 Asylum Collective",
    "book_author": "Asylum",
    "tags": [],
    "start_iso": "2026-02-24T18:00:00",
    "start_time": "6:00 PM",
    "day_of_week_start": "Tue",
    "city_state": "Seattle, WA",
    "thumbnail": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTj9o5J1KePSiSxjdo3uhtiZM1apAUXCItdknWlu99_RX2pB3sLUW_i9rQ&s"
  },
  {
    "event_id": "21176b0fd0368583",
    "title": "Obec Book Club — Ballard Brewed Coalition",
    "link": "https://www.ballardbrewed.com/events/nhq3ga7q0u6s02f4dxzhytqdp0o61d-hyd43-zzxm7-339ge-a8myz-nk9az-eaat5-eatwa-cg9xe-2y764-xcs7p-2azln-rhhnp-a22jj-ew8zg-2ehbb-3hpcg-jhmw6-pw54h-7kkjw",
    "description": "Obec Book Club The book for February will be selected at the January meetup.",
    "book_title": "",
    "book_author": "",
    "tags": [],
    "start_iso": "2026-02-23T18:00:00",
    "start_time": "6:00 PM",
    "day_of_week_start": "Mon",
    "city_state": "Seattle, WA",
    "thumbnail": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQoo3c5BOz0EabYHtE22YhvXJRzsNp8hWBfjAbJvTg&s"
  },
  {
    "event_id": "3ee2c650fc9a6a66",
    "title": "Silent Book Club",
    "link": "https://populusseattle.com/event/silent-book-club/2026-04-16/",
    "description": "Populus Seattle welcomes the global phenomenon Silent Book Club to Pioneer Square. Guests are invited to enjoy a free evening of quiet reading, soft music, and cozy connection in one … Continued\\\\n",
    "book_title": "",
    "book_author": "",
    "tags": [],
    "start_iso": "2026-04-16T18:00:00",
    "start_time": "6:00 PM",
    "day_of_week_start": "Thu",
    "city_state": "Seattle, WA",
    "thumbnail": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQiyeN8OiJzCZS7hIcMfDRvlOrrFmmwV87QYzitRYWJgJ7i07eNkLPzV54&s"
  },
  {
    "event_id": "ad0bb23aaf99a5e8",
    "title": "Book Club - The Sword of Kaigen by M.L. Wang",
    "link": "https://www.meetup.com/fremontfantasy/",
    "description": "Welcome to the Fremont Fantasy Book Club! We will meet once a month in the Fremont area to discuss a book! If you like to read fun books and yap about them, come hang. This is a low stress...",
    "book_title": "The Sword of Kaigen",
    "book_author": "M.L. Wang",
    "tags": [
      "Fantasy"
    ],
    "start_iso": "2026-02-22T12:30:00",
    "start_time": "12:30 PM",
    "day_of_week_start": "Sun",
    "city_state": "Seattle, WA",
    "thumbnail": "https://www.google.com/maps/vt/data=QLd0KA20JQu0iXj2K8cKn5svsGE_SQ-R_Ch3s-ZJ9itwZ4goAQVcQjndo6UuHPhqexnoUeLwcHZLZRy1nbZLBUsYiuL4whjic0L6QYT1gUmGz3ATxjw"
  },
  {
    "event_id": "179611c57910f46e",
    "title": "February Book Club!",
    "link": "https://www.eventbrite.com/e/february-book-club-tickets-1981536108843",
    "description": "Come meet new people and talk about Remarkably Bright Creatures. Enjoy 10% off while you are here for Book Club!",
    "book_title": "",
    "book_author": "",
    "tags": [],
    "start_iso": "2026-02-27T19:00:00",
    "start_time": "7:00 PM",
    "day_of_week_start": "Fri",
    "city_state": "Seattle, WA",
    "thumbnail": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSR-m9MIe6H4RImMsLVeJUMpgwMQ2VlxognwB-Pg08&s"
  },
  {
    "event_id": "94bc2f534220f1b0",
    "title": "March Book Club - \\"Spear\\" by Nicola Griffith",
    "link": "https://www.meetup.com/lesbianlit-117/events/312781735/",
    "description": "A book-club discussion of Spear by Nicola Griffith for readers of queer Arthurian fiction; participants will identify themes and takeaways.",
    "book_title": "Spear",
    "book_author": "Nicola Griffith",
    "tags": [
      "LGBTQ+ Books",
      "Literature & Fiction"
    ],
    "start_iso": "2026-03-14T21:30:00",
    "start_time": "9:30 PM",
    "day_of_week_start": "Sat",
    "city_state": "Seattle, WA",
    "thumbnail": "https://www.google.com/maps/vt/data=uIT_Bg-lzFgcY7KywkIkHce108Vky0_IkaaT9ydgEHvUiQATZ1hy0Fohgpp_dHRuFngyXGmRZE1lPgxbzqBw5UlSMzRknng9DCs4PN0CejjYZy3XSmA"
  },
  {
    "event_id": "89dca06a4080182b",
    "title": "Bookish Romance Trivia with Silent Book Club Capitol Hill and Freeze Tag",
    "link": "https://yotix.in/events/detail/bookish-romance-trivia-with-silent-book-club-capitol-hill-and-freeze-tag-1980232109546",
    "description": "",
    "book_title": "",
    "book_author": "",
    "tags": [
      "Romance"
    ],
    "start_iso": "2026-02-20T02:30:00",
    "start_time": "2:30 AM",
    "day_of_week_start": "Fri",
    "city_state": "Seattle, WA",
    "thumbnail": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSDlEOxKH3g2xAnt2BjYb9RdKL74o4i_SUQaHeXg118y31QiK9Jp38xs_o&s"
  }
]
"""


def recommend_for_user(user_email: str) -> list[dict]:
    """Return the top N recommended events. Skeleton: returns first 10 from embedded data."""
    events = json.loads(_TOP_10_JSON)
    return events
