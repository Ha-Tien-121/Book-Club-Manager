# Interactions

## Use Case 1: Find and join an active book club/event

1. User opens Bookish and optionally signs in.  
2. User enters preferences: genres/topics, neighborhood/location, and availability.  
3. System fetches clubs/events from the Bookclubs Recommender, which queries the Data Manager:
   - SerpAPI event listings (what, when, where, links, thumbnails)
   - Club metadata stored in Bookish (if available)  
4. System ranks results and displays descriptions.  
5. User clicks a club/event to view details.  
6. User taps Join (for Bookish-hosted clubs) or opens an external link for non-hosted clubs.  
7. System updates My Clubs and optionally prompts for notification/calendar preferences.  

flowchart LR
    User --> WebUI
    WebUI --> BookclubsRecommender
    BookclubsRecommender --> DataManager
    DataManager --> SerpAPIData
    BookclubsRecommender --> WebUI
    WebUI --> ClubPage
    ClubPage --> GroupManager
    GroupManager --> MyClubs



## Use Case 2: Get personalized book recommendations and save to Library

1. User visits the Feed tab.  
2. User optionally selects genres/topics and/or imports reading history (or starts with none).  
3. System requests recommendations from the Book Recommender, which combines:
   - Amazon metadata (title, average_rating, rating_number, categories, images)
   - SPL checkout trends (Title, Subjects, Checkouts, PublicationYear)
   - Optional: books currently read by clubs the user follows  
4. Recommender suggests ranked books.  
5. User opens an Individual Book Page and clicks Save, choosing a status: Saved / In Progress / Finished.  
6. System updates the Library, which feeds back into future recommendations.  

flowchart LR
    User --> WebUI
    WebUI --> BookRecommender
    BookRecommender --> DataManager
    DataManager --> AmazonData
    DataManager --> SPLData
    BookRecommender --> WebUI
    WebUI --> BookPage
    BookPage --> Library



## (Extra Implementation) Use Case 3: Coordinate a club reading plan (vote, schedule, and track progress)

1. User opens My Clubs and selects a club.  
2. On the club page, user navigates to Book Vote and proposes a book (search/select from Bookish catalog).  
3. System records votes, shows the current tally, and optionally sets a voting deadline.  
4. After voting ends, an admin confirms the selected book.  
5. User sets or edits the reading schedule (chapters per week and meeting dates).  
6. System writes events to the Calendar component and posts the plan to the club Forum.  
7. Members update reading progress; the club page surfaces aggregate progress to reduce confusion.  

flowchart LR
    User --> WebUI
    WebUI --> MyClubs
    MyClubs --> ClubPage
    ClubPage --> GroupManager
    GroupManager --> DataManager
    ClubPage --> Calendar
    ClubPage --> Forum
    GroupManager --> ClubPage
