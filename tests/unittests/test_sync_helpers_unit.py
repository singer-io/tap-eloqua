import unittest
from unittest.mock import patch

import pendulum

from tap_eloqua.sync import (
    MIN_RETRY_INTERVAL,
    MAX_RETRY_INTERVAL,
    next_sleep_interval,
    get_bookmark,
    get_bulk_bookmark,
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

    def test_pendulum_parse_to_datetime_string_format(self):
        """Validate that pendulum 3.x still produces 'YYYY-MM-DD HH:MM:SS'
        from to_datetime_string(), which the tap uses as the filter value
        sent to the Eloqua API."""
        dt = pendulum.parse("2024-06-01T00:00:00Z")
        result = dt.to_datetime_string()
        # Must be exactly the format used in API filter construction
        self.assertRegex(result, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
        self.assertEqual(result, "2024-06-01 00:00:00")


if __name__ == "__main__":
    unittest.main()
