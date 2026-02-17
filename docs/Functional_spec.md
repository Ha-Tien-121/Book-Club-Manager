# Functional Specifications

## Background

You want to join a book club. You search online and find groups scattered across Facebook, Meetup, Instagram, and random websites. Half the pages are outdated. You can't tell which clubs are active or what they're reading.

You finally join one. Now you're juggling WhatsApp for chat, Google Calendar for meetings, a shared spreadsheet for book votes, and email threads for discussions. Someone suggests a new book. Five people comment "yes" in different messages. Does that mean we're reading it? You have no idea. Three weeks in, you're lost. What chapters are we reading this week? Is everyone else ahead of you? When's the next meeting? You're searching through old messages trying to piece it together.

Most book clubs fail because coordination is exhausting. People want to read together, but managing everything across multiple apps kills the fun. Bookish fixes this. One platform where you find active clubs, vote on books, track your reading progress, discuss chapters, and schedule meetings. No more app-hopping. No more confusion. Just reading and talking about books.

---

## User Profile

Our website is intended for readers who want help finding book clubs, events, and reading communities matching their interests and preferences. The following describes two different types of readers:

### **User Story 1:**

Rayleigh loves reading fantasy novels, but she wants a community to read her books with. She decides to use our app to find a community of readers with similar interests. She inputs her reading history, genre preferences, neighborhood she lives in and availability. She then uses our recommender to find book club / event recommendations that best fit her preferences and engages in forums to talk to others about her current read. She wants an app that is easy and intuitive to use as she does not want to spend a lot of time figuring out another app. 

### **User Story 2:**

Ben is a software engineer hoping to make a career pivot into UX/UI design. He wants to learn more about the field and is practicing diligently by himself. However, he feels there is a gap in his knowledge by studying alone so he is seeking learning communities for UX/UI. Specifically, he is looking for a book club on UX/UI books and hoping to have productive conversations with other peers in the field. He knows how to use the internet and is very adept at using websites. He comes upon the app on recommendation by his colleague. He wants to use it to find a professional UI/UX book club in his area. He wants an app that will be convenient and user-friendly to use. And he wants to find the right club that fits into his busy life.  

Users could range from casual readers, such as Raleigh who want recommendations based on their reading preferences, location, and availability, to professionals looking for topic-specific communities, such as Ben who want more targeted recommendations based on career and personal interests. Users do not require programming or technical experience, but they should be comfortable navigating websites and have a basic understanding of books and reading communities. 

---

## Data Sources

### **McAuley Lab Amazon Reviews 2023**

#### **Meta Data:**  
File Type: jsonl  

**Columns**  
_*Note: columns of dataset we are keeping are highlighted*_

- Field: main_category  
  - Type: str  
  - Explanation: Main category (i.e., domain) of the product.

- ==Field: title==  
  - Type: str  
  - Explanation: Name of the product.

- ==Field: average_rating==  
  - Type: float  
  - Explanation: Rating of the product shown on the product page.

- ==Field: rating_number==  
  - Type: int  
  - Explanation: Number of ratings for the product.

- Field: features  
  - Type: list  
  - Explanation: Bullet-point features of the product.

- Field: description  
  - Type: list  
  - Explanation: Description of the product.

- Field: price  
  - Type: float  
  - Explanation: Price in US dollars (at time of crawling).

- ==Field: images==  
  - Type: list  
  - Explanation: Images of the product. Each image includes multiple sizes (thumb, large, hi_res). The “variant” field indicates the image position.

- Field: videos  
  - Type: list  
  - Explanation: Videos of the product, including title and URL.

- Field: store  
  - Type: str  
  - Explanation: Store name of the product.

- ==Field: categories==  
  - Type: list  
  - Explanation: Hierarchical categories of the product.

- Field: details  
  - Type: dict  
  - Explanation: Product details such as materials, brand, sizes, etc.

- ==Field: parent_asin==  
  - Type: str  
  - Explanation: Parent ID of the product.

- Field: bought_together  
  - Type: list  
  - Explanation: Recommended product bundles from the website.

---

#### **Reviews Data**  
File Type: jsonl  

**Columns**  
_*Note: columns of dataset we are keeping are highlighted and columns used to filter data bolded*_

- **Field: rating**  
  - Type: float  
  - Explanation: Rating of the product (from 1.0 to 5.0).

- Field: title  
  - Type: str  
  - Explanation: Title of the user review.

- Field: text  
  - Type: str  
  - Explanation: Text body of the user review.

- Field: images  
  - Type: list  
  - Explanation: Images that users post after they have received the product. Each image includes multiple sizes (small, medium, large), represented by small_image_url, medium_image_url, and large_image_url.

- Field: asin  
  - Type: str  
  - Explanation: ID of the product.

- ==Field: parent_asin==  
  - Type: str  
  - Explanation: Parent ID of the product. Products with different colors, styles, or sizes usually share the same parent ID. Note: In previous Amazon datasets, the “asin” field often corresponds to the parent ID. Use the parent ID to find product metadata.

- ==Field: user_id==  
  - Type: str  
  - Explanation: ID of the reviewer.

- Field: timestamp  
  - Type: int  
  - Explanation: Time of the review in Unix time.

- Field: verified_purchase  
  - Type: bool  
  - Explanation: Indicates whether the reviewer is a verified purchaser.

- Field: helpful_vote  
  - Type: int  
  - Explanation: Number of helpful votes received by the review.

---

### **SPL Data**

#### **Checkout by Title**  
_*only fetched data from last year, to the month*_

- Field: UsageClass  
  - Type: Text  
  - Explanation: Denotes whether the item is physical or digital.

- Field: CheckoutType  
  - Type: Text  
  - Explanation: Denotes the vendor tool used to check out the item.

- Field: MaterialType  
  - Type: Text  
  - Explanation: Describes the type of item checked out (for example: book, song, movie, music, magazine).

- Field: CheckoutYear  
  - Type: Number  
  - Explanation: The 4-digit year in which the checkout occurred.

- Field: CheckoutMonth  
  - Type: Number  
  - Explanation: The month in which the checkout occurred.

- Field: Checkouts  
  - Type: Number  
  - Explanation: The number of times the title was checked out during the checkout month.

- Field: Title  
  - Type: Text  
  - Explanation: The full title and subtitle of the item.

- Field: ISBN  
  - Type: Text  
  - Explanation: A comma-separated list of ISBNs associated with the item record.

- Field: Creator  
  - Type: Text  
  - Explanation: The author or entity responsible for creating the item.

- Field: Subjects  
  - Type: Text  
  - Explanation: The subject of the item as it appears in the catalog.

- Field: Publisher  
  - Type: Text  
  - Explanation: The publisher of the title.

- Field: PublicationYear  
  - Type: Text  
  - Explanation: The year the item was published, printed, or copyrighted according to the catalog record.

---

#### **Library Collections**  
_*Only fetched from last year*_

- Field: BibNum  
  - Type: Number  
  - Explanation: The unique identifier for a cataloged item within the Library's Integrated Library System (ILS).

- Field: Title  
  - Type: Text  
  - Explanation: The full title of an item.

- Field: Author  
  - Type: Text  
  - Explanation: The name of the first author of the title, if applicable.

- Field: ISBN  
  - Type: Text  
  - Explanation: A comma-delimited list of ISBN(s) for this title.

- Field: PublicationYear  
  - Type: Text  
  - Explanation: The year of publication.

- Field: Publisher  
  - Type: Text  
  - Explanation: The name of the publishing company for this item.

- Field: Subjects  
  - Type: Text  
  - Explanation: A comma-separated list of subject authority records associated with the title (for example, Motion Pictures, Computer Programming). These are typically highly specific.

- Field: ItemType  
  - Type: Text  
  - Explanation: Horizon item type.

- Field: ItemCollection  
  - Type: Text  
  - Explanation: Collection code for this item.

- Field: FloatingItem  
  - Type: Text  
  - Explanation: Indicates whether an item floats.

- Field: ItemLocation  
  - Type: Text  
  - Explanation: The 3-letter code for the location that owned the item at the time of the snapshot.

- Field: ReportDate  
  - Type: Floating Timestamp  
  - Explanation: The date when the item count was collected from the ILS (Horizon).

- Field: ItemCount  
  - Type: Number  
  - Explanation: The number of items at this location, collection, item type, and item status as of the report date.

---

### **SerpAPI Data**

- Field: query  
  - Type: Text  
  - Explanation: The SerpAPI query input to get bookclub events. 

- Field: title  
  - Type: Text  
  - Explanation: The name of the bookclub.

- Field: link  
  - Type: url  
  - Explanation: Link to bookclub event listing.

- Field: description  
  - Type: Text  
  - Explanation: Description of bookclub.

- Field: when  
  - Type: Datetime string  
  - Explanation: Time of bookclub.

- Field: start_date  
  - Type: Date string  
  - Explanation: Date the bookclub started on.

- Field: end_date  
  - Type: Date string  
  - Explanation: Date the bookclub ended/ends on.
	
- Fieldaddress  
  - Type: List, [address, Seattle WA]  
  - Explanation: Address of bookclub. 

- Field: venue  
  - Type: List, [name, rating, reviews, link]  
  - Explanation: Gives name of bookclub, its ratings, number of reviews, and a link to the bookclub event.

- Field: Location  
  - Type: ? (empty for all rows)  
  - Explanation: Needs to be removed.

- Field: thumbnail  
  - Type: url  
  - Explanation: Url of bookclub thumbnail. 

---

## Use Cases

### Use Case 1: Find and join an active book club/event

**Objective:**  
The user wants to find a book club/event that aligns with their interests (genre), schedule, and location. 

**Interactions:**
- User opens Bookish and signs in (optional).
- User enters preferences: genres/topics, neighborhood/location, and availability.
- System fetches clubs/events from the Bookclubs Recommender, which queries the data manager:
  - SerpAPI event listings (what/when/where, links, thumbnails)
  - Club metadata stored in Bookish (if possible)
- System ranks results and shows a description.
- User clicks a club/event to view details.
- User taps Join (for Bookish-hosted clubs) or open external link for non-hosted clubs.
- System updates “My Clubs” and (optional) prompts for notification/calendar preferences.

---

### Use Case 2: Get personalized book recommendations and save to Library

**Objective:**  
The user wants personalized book recommendations and a way to store and organize reading progress. 

**Interactions:**
- User visits the Feed tab.
- User optionally selects genres/topics and/or imports reading history (or starts with none).
- System requests recommendations from Book Recommender, which combines:
  - Amazon metadata (title, average_rating, rating_number, categories, images)
  - SPL checkout trends (Title, Subjects, Checkouts, PublicationYear)
  - (Optional) “books currently read by clubs the user follows” from Bookish clubs
- Recommender suggests books.
- User opens an Individual Book Page and clicks Save → chooses status: Saved / In Progress / Finished.
- System updates Library, which feeds back into future recommendations.

---

### (Extra Implementation) Use Case 3: Coordinate a club reading plan (vote, schedule, and track progress)

**Objective:**  
The user wants to coordinate book selections and reading schedules within their book club in a single integrated platform.

**Interactions:**
- A user opens My Clubs → selects a club.
- In the club page, user navigates to Book Vote and proposes a book (search/select from Bookish catalog).
- System records votes, shows current tally, and (optionally) sets a voting deadline.
- After voting ends, an admin confirms the selected book.
- User sets or edits the reading schedule (chapters per week + meeting dates).
- System writes events to the Calendar component and posts the plan to the club’s Forum.
- Members update reading progress; the club page surfaces “where most people are” to reduce confusion.
