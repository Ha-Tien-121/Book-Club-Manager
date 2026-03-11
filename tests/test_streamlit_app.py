"""
UI tests for the Bookish Streamlit app (streamlit_app.py).

Uses Streamlit's AppTest API. Run from project root, e.g.:
  python -m pytest tests/test_streamlit_app.py -v
  python -m unittest tests.test_streamlit_app -v

See: https://docs.streamlit.io/library/advanced-features/app-testing
"""

from pathlib import Path
import unittest

from streamlit.testing.v1 import AppTest


def _app_path() -> str:
    """Path to streamlit_app.py from repo root."""
    return str(Path(__file__).resolve().parent.parent / "streamlit_app.py")


class StreamlitAppTest(unittest.TestCase):
    """Tests for main Bookish Streamlit UI."""

    def setUp(self) -> None:
        self.at = AppTest.from_file(_app_path()).run()

    def _find_text_input(self, label: str):
        """Return the first text_input element matching label (sidebar or main)."""
        candidates = []
        if hasattr(self.at, "sidebar") and hasattr(self.at.sidebar, "text_input"):
            candidates.extend(list(self.at.sidebar.text_input))
        candidates.extend(list(self.at.text_input))
        for el in candidates:
            if getattr(el, "label", None) == label:
                return el
        raise AssertionError(f"text_input with label '{label}' not found")

    def test_app_runs_without_error(self) -> None:
        """App loads and completes one run."""
        self.assertIsNotNone(self.at)

    def test_feed_title_present(self) -> None:
        """Feed tab shows main heading 'Discover your next read'."""
        titles = [t.value for t in self.at.title]
        self.assertIn("Discover your next read", titles)

    def test_feed_section_headings(self) -> None:
        """Feed contains Trending, Recommended, and Suggested sections."""
        parts = []
        for el in list(self.at.subheader) + list(self.at.markdown):
            parts.append(getattr(el, "value", str(el)))
        all_text = " ".join(parts)
        self.assertIn("Trending in Seattle", all_text)
        self.assertIn("Recommended for you", all_text)
        self.assertIn("Suggested book clubs", all_text)

    def test_see_more_clubs_button(self) -> None:
        """Feed has 'See More Clubs' button."""
        button_labels = [b.label for b in self.at.button]
        self.assertIn("See More Clubs", button_labels)

    def test_tabs_present(self) -> None:
        """Main navigation has expected tabs."""
        self.assertGreaterEqual(len(self.at.tabs), 1)
        tab_labels = [t.label for t in self.at.tabs]
        expected = {"Feed", "Explore Clubs", "My Clubs", "Library", "Forum"}
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
        email_input = self._find_text_input("Email")
        password_input = self._find_text_input("Password")

        email_input.input("abc@gmail.com").run()
        password_input.input("123456").run()

        create_buttons = [b for b in self.at.button if b.label == "Create Account"]
        self.assertGreaterEqual(len(create_buttons), 1, "Create Account button not found")
        create_buttons[0].click().run()

        error_text = " ".join(
            [getattr(e, "value", "") for e in list(self.at.error)]
            + [
                getattr(e, "value", "")
                for e in list(getattr(self.at.sidebar, "error", []))
            ]
        )
        self.assertIn("Email has been taken.", error_text)


class StreamlitAppExploreClubsTest(unittest.TestCase):
    """Tests for Explore Clubs tab (tab label and presence)."""

    def setUp(self) -> None:
        self.at = AppTest.from_file(_app_path()).run()

    def test_explore_clubs_tab_label(self) -> None:
        """Explore Clubs tab exists in navigation."""
        tab_labels = [t.label for t in self.at.tabs]
        self.assertIn("Explore Clubs", tab_labels)


class StreamlitAppForumTest(unittest.TestCase):
    """Tests for Forum tab (tab label and presence)."""

    def setUp(self) -> None:
        self.at = AppTest.from_file(_app_path()).run()

    def test_forum_tab_label(self) -> None:
        """Forum tab exists in navigation."""
        tab_labels = [t.label for t in self.at.tabs]
        self.assertIn("Forum", tab_labels)


if __name__ == "__main__":
    unittest.main()
