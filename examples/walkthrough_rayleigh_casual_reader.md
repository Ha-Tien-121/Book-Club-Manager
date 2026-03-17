## Walkthrough: Rayleigh (Casual Reader)

### Persona

Rayleigh enjoys fantasy novels and wants a community of like-minded readers. She values simplicity and ease of use.

### Goals (from Functional Spec)

- Input reading preferences
- Discover book clubs/events in her area
- Join discussions and engage with others
- Save books to her Library and get better recommendations

---

## Scenario A — Discover and Join a Book Club (Use Case 1)

### Steps (in the UI)

1. Open the app and sign in (or create an account if new).
2. Go to **Explore Events**.
3. In **City**, choose a relevant location (or keep **All**).
4. In **Filter by genre tags**, select tags related to fantasy (e.g., “Fantasy”).
5. Review the filtered event list:
   - Read the event description summary
   - Check the “When” line
   - Check tags shown as pills
6. Click **Save event** on an event she wants to join.

### Expected results

- The event is marked as **Saved**
- The app returns her to the correct tab on rerun
- The saved event appears in **My Events**

---

## Scenario B — Personalized Book Recommendations + Library (Use Case 2)

### Steps (in the UI)

1. Go to **Feed**.
2. Review **Trending in Seattle** (popular titles).
3. Review **Recommended for you**.
4. Click a book card or open **Book Detail**.
5. In Book Detail, set **Library status** to:
   - Saved, or
   - In Progress, or
   - Finished

### Expected results

- The library status updates successfully
- The saved/finished/in-progress book appears under the **Library** tab in the correct shelf
- Future recommendations shift over time as she saves/finishes books

---

## Scenario C — Participate in discussions (Forum)

### Steps (in the UI)

1. Go to **Forum**.
2. Click **Create a discussion**.
3. Enter:
   - Title
   - Post content
   - Optional tags (comma-separated)
4. Submit the post.
5. From the forum list, click **Open discussion**.
6. Add a reply using **Write a reply**.

### Expected results

- A new forum post appears at the top of the list
- Opening the discussion shows the detail view
- Replies appear under “Comments”
- Like/Save actions are available when signed in
