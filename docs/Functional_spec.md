# Functional Specifications

## Background

Finding and managing a book club today is fragmented and inefficient.  
Users often search across multiple platforms such as Meetup, Facebook, and forums, only to find outdated or incomplete information.

Once they join a club, coordination becomes difficult:

- Discussions happen across multiple apps
- Schedules are unclear
- Reading progress is not tracked centrally

This fragmentation leads to poor engagement and often causes book clubs to fail.

**Bookish addresses this problem by providing a unified platform** where users can:

- Discover active book clubs and events
- Track reading progress
- Participate in discussions
- Receive personalized book recommendations

---

## User Profiles

### User Story 1: Casual Reader (Rayleigh)

Rayleigh enjoys reading fantasy novels and wants to find a community of like-minded readers.

She uses Bookish to:

- Input her reading preferences
- Discover book clubs in her area
- Join discussions and engage with others

She values simplicity and ease of use.

---

### User Story 2: Professional Learner (Ben)

Ben is a software engineer transitioning into UX/UI design.

He uses Bookish to:

- Find topic-specific book clubs
- Engage in discussions with professionals
- Learn collaboratively

He values efficiency and targeted recommendations.

---

## Data Sources

### Amazon Books Dataset

Used for book metadata and recommendation signals.

**Key Fields:**

- `title`
- `average_rating`
- `rating_number`
- `description`
- `images`
- `categories`
- `parent_asin`

---

### SPL (Seattle Public Library) Data

Used for local popularity and availability signals.

**Key Fields:**

- `Checkouts`
- `Title`
- `Subjects`
- `PublicationYear`

---

### Event Data

Used for discovering book clubs and events.

**Key Fields:**

- `title`
- `description`
- `date/time`
- `location`
- `link`
- `venue`

---

## Use Cases

### Use Case 1: Discover and Join a Book Club

**Objective:**  
Find a book club based on preferences.

**Flow:**

1. User enters preferences (genre, location)
2. System retrieves and ranks events
3. User views details
4. User joins event
5. Event added to “My Events”

---

### Use Case 2: Personalized Book Recommendations

**Objective:**  
Receive and manage book recommendations.

**Flow:**

1. User visits Feed
2. System generates recommendations
3. User explores book details
4. User saves book to Library
5. Library updates future recommendations
