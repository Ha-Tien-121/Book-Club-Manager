# Functional Specification

## 1. Background

Many people want to join book clubs but face significant coordination and discovery challenges. Book clubs are scattered across platforms such as Facebook, Meetup, Instagram, and independent websites. Information is often outdated, incomplete, or unclear regarding whether a club is active, what book is currently being read, and when meetings occur.

After joining a club, members typically rely on multiple disconnected tools—such as WhatsApp for chat, Google Calendar for meetings, shared spreadsheets for voting, and email threads for discussion. This fragmentation leads to confusion around:

- Which book has officially been selected
- What chapters are being read each week
- When the next meeting occurs
- Who is participating and at what pace

As a result, coordination becomes exhausting, and many book clubs fail despite strong interest in shared reading experiences.

**Bookish** addresses this problem by offering a unified platform where users can:
- Discover active book clubs and events
- Receive personalized book recommendations
- Vote on books within clubs
- Track reading progress
- Participate in discussions
- Schedule and manage meetings

The goal is to eliminate app fragmentation and reduce coordination friction so users can focus on reading and meaningful discussion.

---

## 2. User Profile

Bookish serves a broad range of readers seeking community and structure. Users are expected to be comfortable navigating websites but do not require programming or advanced technical skills.

### User Type 1: Casual Genre Reader (e.g., Rayleigh)

- Enjoys reading (e.g., fantasy novels)
- Wants to find a community with similar interests
- Inputs:
  - Reading history
  - Genre preferences
  - Location
  - Availability
- Uses:
  - Book club recommender
  - Discussion forums
  - Reading tracker
- Expectations:
  - Simple, intuitive interface
  - Minimal setup effort
  - Personalized recommendations

Technical proficiency:
- Comfortable browsing the web
- No programming knowledge required

---

### User Type 2: Professional Skill-Focused Reader (e.g., Ben)

- Software engineer pivoting to UX/UI
- Seeks topic-specific learning communities
- Looking for:
  - Professional or subject-focused book clubs
  - Structured discussions
- Values:
  - Efficiency
  - Strong filtering tools
  - Clubs that fit into a busy schedule

Technical proficiency:
- Highly comfortable with web applications
- Technically adept but does not require developer tools

---

### General User Characteristics

- Range from casual readers to professionals
- Comfortable navigating modern web apps
- No coding knowledge required
- Interested in books, reading trends, and community participation

---

## 3. Data Sources

Bookish integrates multiple data sources to power recommendations, discovery, and coordination features.

---

### 3.1 McAuley Lab Amazon Reviews 2023

**File Type:** JSONL

#### Metadata (Selected Fields)

- `title` (str): Name of the book
- `average_rating` (float): Product rating
- `rating_number` (int): Number of ratings
- `images` (list): Book cover images (multiple sizes)
- `categories` (list): Hierarchical genre/category labels
- `parent_asin` (str): Parent product ID

Used for:
- Book metadata
- Popularity scoring
- Rating-based ranking
- Genre classification
- Book imagery

---

#### Reviews Data (Selected Fields)

- `rating` (float): Review rating (1.0–5.0)
- `parent_asin` (str): Product identifier
- `user_id` (str): Reviewer ID
- `verified_purchase` (bool)
- `helpful_vote` (int)

Used for:
- Popularity metrics
- Rating aggregation
- Collaborative filtering signals (e.g., KNN)

---

### 3.2 Seattle Public Library (SPL) Data

#### Checkout by Title (Last Year)

- `Title`
- `Checkouts`
- `Subjects`
- `PublicationYear`
- `ISBN`
- `Creator`

Used for:
- Trend detection
- Local popularity ranking
- Genre/topic classification
- Recency-based boosting

---

#### Library Collections (Last Year)

- `Title`
- `Author`
- `ISBN`
- `PublicationYear`
- `Subjects`
- `ItemLocation`
- `ItemCount`

Used for:
- Availability detection
- Local library access signals
- Subject enrichment

---

### 3.3 SerpAPI Data (Book Club Events)

- `query`
- `title`
- `link`
- `description`
- `when`
- `start_date`
- `end_date`
- `address`
- `venue`
- `thumbnail`

Used for:
- Discovering external book club events
- Time and location filtering
- Ranking clubs by relevance
- Linking to external listings

(Note: Empty `Location` field will be removed during preprocessing.)

---

## 4. Use Cases

---

### Use Case 1: Find and Join an Active Book Club/Event

#### Objective

The user wants to find a book club that aligns with:
- Genre interests
- Location
- Availability

#### Interactions

1. User opens Bookish (sign-in optional).
2. User enters:
   - Genre/topics
   - Neighborhood/location
   - Availability
3. System queries:
   - SerpAPI event listings
   - Internal Bookish club database
4. Recommender ranks results.
5. System displays club/event list.
6. User selects a club/event.
7. User:
   - Clicks **Join** (Bookish-hosted), or
   - Opens external link
8. System:
   - Adds club to “My Clubs”
   - Optionally prompts for notifications/calendar sync

---

### Use Case 2: Get Personalized Book Recommendations and Save to Library

#### Objective

The user wants book recommendations tailored to their preferences and a way to track reading progress.

#### Interactions

1. User visits the **Feed** tab.
2. User optionally:
   - Selects genres/topics
   - Imports reading history
3. System requests recommendations from Book Recommender.

Recommender integrates:

- Amazon metadata:
  - `average_rating`
  - `rating_number`
  - `categories`
  - `images`
- SPL checkout trends:
  - `Checkouts`
  - `Subjects`
  - `PublicationYear`
- Books currently read by followed clubs (optional)

4. System ranks books and suggests recommendations
5. User opens an Individual Book Page.
6. User selects:
   - **Saved**
   - **In Progress**
   - **Finished**
7. System updates **Library**.
8. Library data feeds back into recommender to improve personalization.

---

### Use Case 3: Coordinate a Club Reading Plan (Vote, Schedule, Track)

#### Objective

Users want to coordinate book selection and reading schedules within a single integrated platform.

#### Interactions

1. User navigates to **My Clubs** → selects a club.
2. User goes to **Book Vote**.
3. User proposes a book (searches Bookish catalog).
4. System:
   - Records votes
   - Displays real-time tally
   - Optionally sets voting deadline
5. After voting:
   - Admin confirms selected book
6. Admin sets reading schedule:
   - Chapters per week
   - Meeting dates
7. System:
   - Updates Calendar component
   - Posts schedule to Forum
8. Members update reading progress.
9. Club dashboard displays:
   - Aggregate progress
   - “Most members are here” indicator
