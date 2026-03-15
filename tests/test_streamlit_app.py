"""
UI tests for the Bookish Streamlit app (streamlit_app.py).

Uses Streamlit's AppTest API. Run from project root, e.g.:
  python -m pytest tests/test_streamlit_app.py -v
  python -m unittest tests.test_streamlit_app -v

See: https://docs.streamlit.io/library/advanced-features/app-testing
"""

from pathlib import Path
import unittest
from uuid import uuid4

from streamlit.testing.v1 import AppTest
from backend.services.auth_service import create_user
from backend.storage import get_storage


def _app_path() -> str:
    """Path to streamlit_app.py from repo root."""
    return str(Path(__file__).resolve().parent.parent / "streamlit_app.py")


class StreamlitAppTest(unittest.TestCase):
    """Tests for main Bookish Streamlit UI."""

    timeout = 10

    def setUp(self) -> None:
        self.at = self._run_app()

    def _run_app(self, *, session_state: dict | None = None):
        """Run app with optional seed session state."""
        at = AppTest.from_file(_app_path())
        for key, value in (session_state or {}).items():
            at.session_state[key] = value
        return at.run(timeout=self.timeout)

    @staticmethod
    def _button_labels(at) -> list[str]:
        """Return all rendered button labels."""
        return [getattr(btn, "label", "") for btn in at.button]

    @staticmethod
    def _all_text(at) -> str:
        """Flatten major text-bearing elements into one searchable string."""
        parts = []
        for collection in (
            at.title,
            at.subheader,
            at.markdown,
            at.caption,
            at.success,
            at.warning,
            at.info,
            at.error,
        ):
            for el in list(collection):
                parts.append(getattr(el, "value", ""))
        return " ".join(str(p) for p in parts if p)

    def test_app_runs_without_error(self) -> None:
        """App loads and completes one run."""
        self.assertIsNotNone(self.at)

    def test_feed_title_present(self) -> None:
        """Feed tab shows main heading 'Discover your next read'."""
        titles = [t.value for t in self.at.title]
        self.assertIn("Discover your next read", titles)

    def test_feed_section_headings(self) -> None:
        """Feed contains Trending, Recommended, and Suggested sections."""
        all_text = self._all_text(self.at)
        self.assertIn("Trending in Seattle", all_text)
        self.assertIn("Recommended for you", all_text)
        self.assertIn("Suggested events", all_text)

    def test_see_more_events_button(self) -> None:
        """Feed has 'See More Events' button."""
        self.assertIn("See More Events", self._button_labels(self.at))

    def test_tabs_present(self) -> None:
        """Main navigation has expected tabs."""
        self.assertGreaterEqual(len(self.at.tabs), 1)
        tab_labels = [t.label for t in self.at.tabs]
        expected = {"Feed", "Explore Events", "My Events", "Library", "Forum"}
        for label in expected:
            self.assertIn(label, tab_labels, f"Tab '{label}' not found in {tab_labels}")

    def test_sidebar_bookish_title(self) -> None:
        """Sidebar shows app name 'Bookish'."""
        sidebar_text_parts = []
        if hasattr(self.at.sidebar, "title") and self.at.sidebar.title:
            sidebar_text_parts.extend(getattr(t, "value", "") for t in self.at.sidebar.title)
        for el in list(self.at.sidebar.markdown):
            sidebar_text_parts.append(getattr(el, "value", str(el)))
        sidebar_text = " ".join(sidebar_text_parts)
        self.assertIn("Bookish", sidebar_text)

    def test_signed_out_auth_controls_present(self) -> None:
        """Signed-out state shows sign-in and create-account controls."""
        all_labels = self._button_labels(self.at)
        self.assertIn("Sign in", all_labels)
        self.assertIn("Create account", all_labels)

    def test_multiselect_genre_filter(self) -> None:
        """Feed has genre filter multiselect."""
        self.assertGreaterEqual(len(self.at.multiselect), 1)
        # First multiselect is "Filter by genre"
        self.assertEqual(self.at.multiselect[0].label, "Filter by genre")

    def test_library_and_forum_tabs_in_nav(self) -> None:
        """Library and Forum tabs are in main tab list."""
        tab_labels = [t.label for t in self.at.tabs]
        self.assertIn("Library", tab_labels)
        self.assertIn("Forum", tab_labels)

    def test_create_account_duplicate_email_shows_error(self) -> None:
        """Creating an account with an existing email shows 'Email has been taken.'."""
        duplicate_email = f"ui-test-{uuid4().hex[:10]}@example.com"
        create_user(email=duplicate_email, password="password123")

        at = self._run_app(session_state={"show_create_account": True})

        email_input = next(el for el in at.text_input if el.label == "Email")
        password_input = next(el for el in at.text_input if el.label == "Password")
        email_input.input(duplicate_email).run(timeout=10)
        password_input.input("123456").run(timeout=10)

        submitters = [b for b in at.button if b.label == "Create account"]
        self.assertGreaterEqual(
            len(submitters), 1, "Create account form submit button not found"
        )
        submitters[0].click().run(timeout=10)

        error_text = " ".join(
            [getattr(e, "value", "") for e in list(at.error)]
            + [
                getattr(e, "value", "")
                for e in list(getattr(at.sidebar, "error", []))
            ]
        )
        self.assertIn("Email has been taken.", error_text)

    def test_create_account_page_controls_present(self) -> None:
        """Create account page renders expected form controls."""
        at = self._run_app(session_state={"show_create_account": True})
        titles = [t.value for t in at.title]
        self.assertIn("Create account", titles)
        input_labels = [ti.label for ti in at.text_input]
        self.assertIn("Email", input_labels)
        self.assertIn("Password", input_labels)
        self.assertIn("Display name (optional)", input_labels)
        self.assertIn("Create account", self._button_labels(at))

    def test_forum_detail_page_renders_for_selected_post(self) -> None:
        """Selected forum post id routes into discussion detail UI."""
        forum_posts = get_storage().load_forum_db().get("posts", [])
        self.assertGreater(len(forum_posts), 0, "Expected at least one forum post fixture")
        post_id = int(forum_posts[0]["id"])

        at = self._run_app(session_state={"selected_forum_post_id": post_id})
        all_text = self._all_text(at)
        self.assertIn("Comments", all_text)
        self.assertTrue(
            any("Back to Forum" in label for label in self._button_labels(at)),
            "Back-to-forum button not found",
        )


class StreamlitAppExploreEventsTest(unittest.TestCase):
    """Tests for Explore Events tab (tab label and presence)."""

    def setUp(self) -> None:
        self.at = AppTest.from_file(_app_path()).run(timeout=10)

    def test_explore_events_tab_label(self) -> None:
        """Explore Events tab exists in navigation."""
        tab_labels = [t.label for t in self.at.tabs]
        self.assertIn("Explore Events", tab_labels)


class StreamlitAppForumTest(unittest.TestCase):
    """Tests for Forum tab (tab label and presence)."""

    def setUp(self) -> None:
        self.at = AppTest.from_file(_app_path()).run(timeout=10)

    def test_forum_tab_label(self) -> None:
        """Forum tab exists in navigation."""
        tab_labels = [t.label for t in self.at.tabs]
        self.assertIn("Forum", tab_labels)

    def test_forum_create_controls_visible(self) -> None:
        """Forum list view includes discussion composer controls."""
        all_text = " ".join(getattr(el, "value", "") for el in self.at.caption)
        self.assertIn("Sign in to create and save forum posts.", all_text)


if __name__ == "__main__":
    unittest.main()
