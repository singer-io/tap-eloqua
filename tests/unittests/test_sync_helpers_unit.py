import unittest
from unittest.mock import patch

import pendulum

from tap_eloqua.sync import (
    MIN_RETRY_INTERVAL,
    MAX_RETRY_INTERVAL,
    next_sleep_interval,
    get_bookmark,
    get_bulk_bookmark,
    _to_eloqua_datetime,
)


class TestSyncHelpersUnit(unittest.TestCase):
    @patch("tap_eloqua.sync.random.randint", return_value=2)
    def test_next_sleep_interval_uses_min_on_first_call(self, mock_randint):
        value = next_sleep_interval(0)
        self.assertEqual(value, MIN_RETRY_INTERVAL)
        mock_randint.assert_called_once_with(MIN_RETRY_INTERVAL, MIN_RETRY_INTERVAL)

    @patch("tap_eloqua.sync.random.randint", return_value=7)
    def test_next_sleep_interval_doubles_until_cap(self, mock_randint):
        value = next_sleep_interval(5)
        self.assertEqual(value, 7)
        mock_randint.assert_called_once_with(5, 10)

    @patch("tap_eloqua.sync.random.randint", return_value=600)
    def test_next_sleep_interval_caps_at_max(self, mock_randint):
        value = next_sleep_interval(MAX_RETRY_INTERVAL)
        self.assertEqual(value, MAX_RETRY_INTERVAL)
        mock_randint.assert_called_once_with(MAX_RETRY_INTERVAL, MAX_RETRY_INTERVAL * 2)

    def test_get_bookmark_with_and_without_value(self):
        state = {"bookmarks": {"accounts": "2024-01-01T00:00:00Z"}}
        self.assertEqual(get_bookmark(state, "accounts", {}), "2024-01-01T00:00:00Z")
        self.assertEqual(get_bookmark(state, "contacts", "default"), "default")

    def test_get_bulk_bookmark_supports_string_and_object(self):
        string_state = {"bookmarks": {"accounts": "2024-01-01T00:00:00Z"}}
        object_state = {
            "bookmarks": {
                "accounts": {
                    "datetime": "2024-01-02T00:00:00Z",
                    "sync_id": "12",
                    "offset": 100,
                }
            }
        }

        self.assertEqual(
            get_bulk_bookmark(string_state, "accounts"),
            {"datetime": "2024-01-01T00:00:00Z"},
        )
        self.assertEqual(
            get_bulk_bookmark(object_state, "accounts"),
            {
                "datetime": "2024-01-02T00:00:00Z",
                "sync_id": "12",
                "offset": 100,
            },
        )

    def test_to_eloqua_datetime_from_string(self):
        """_to_eloqua_datetime produces YYYY-MM-DD HH:MM:SS from an RFC-3339 string.
        Uses explicit .format() so behaviour is not tied to the deprecated
        to_datetime_string() and is safe across Pendulum 2.x / 3.x."""
        result = _to_eloqua_datetime("2024-06-01T00:00:00Z")
        self.assertRegex(result, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
        self.assertEqual(result, "2024-06-01 00:00:00")

    def test_to_eloqua_datetime_from_pendulum_object(self):
        """_to_eloqua_datetime accepts a pendulum DateTime directly
        (used for the end_date argument in sync_bulk_obj)."""
        dt = pendulum.parse("2024-08-15T12:30:45Z")
        result = _to_eloqua_datetime(dt)
        self.assertRegex(result, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
        self.assertEqual(result, "2024-08-15 12:30:45")


if __name__ == "__main__":
    unittest.main()
