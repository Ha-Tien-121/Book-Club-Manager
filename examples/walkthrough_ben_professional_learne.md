## Walkthrough: Ben (Professional Learner)

### Persona

Ben is a software engineer transitioning into UX/UI design. He values efficiency and targeted recommendations.

### Goals (from Functional Spec)

- Find topic-specific clubs/events
- Engage in discussions with professionals
- Learn collaboratively through reading + discussion
- Use recommendations to find relevant books

---

## Scenario A — Targeted event discovery (Use Case 1)

### Steps (in the UI)

1. Sign in.
2. Go to **Explore Events**.
3. Use **City** to narrow to relevant in-person/online location context (if applicable).
4. Use **Filter by genre tags** to target learning goals (e.g., “Design”, “UX”, “Product”, or similar tags if available).
5. Open event listings via **Open event listing** when available.
6. Click **Save event** for the most relevant groups.

### Expected results

- Ben sees a narrowed list of events based on filters
- Saved events appear in **My Events**
- Removing an event in **My Events** removes it from his saved list

---

## Scenario B — Get and manage recommendations (Use Case 2)

### Steps (in the UI)

1. Go to **Feed**.
2. Review **Recommended for you**.
3. Use **Filter by genre** to focus the list (when applicable).
4. Open **Book Detail** for a recommendation.
5. Add the book to **Library** with an appropriate status (Saved / In Progress / Finished).

### Expected results

- Book detail renders consistently and shows key metadata
- Library status changes are reflected under **Library**
- Recommendations become more aligned over time as Ben’s library history grows

---

## Scenario C — Collaborative learning via forum

### Steps (in the UI)

1. Go to **Forum**.
2. Search discussions by tag using **Search by tags** (e.g., “design”, “ux”, “product”).
3. Open a relevant discussion.
4. Like/save useful posts and add replies.

### Expected results

- Tag search filters down discussions
- Ben can open, reply, and react to discussions when signed in
