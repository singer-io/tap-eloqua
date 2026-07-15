import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from singer.catalog import Catalog

import pendulum

from tap_eloqua.sync import (
    MIN_RETRY_INTERVAL,
    MAX_RETRY_INTERVAL,
    next_sleep_interval,
    get_bookmark,
    get_bulk_bookmark,
    sync_static_endpoint,
    sync_activity_stream,
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

    def test_pendulum_parse_to_datetime_string_from_string(self):
        """pendulum.parse().to_datetime_string() produces YYYY-MM-DD HH:MM:SS
        from an RFC-3339 string (replaces _to_eloqua_datetime for string inputs)."""
        result = pendulum.parse("2024-06-01T00:00:00Z").to_datetime_string()
        self.assertRegex(result, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
        self.assertEqual(result, "2024-06-01 00:00:00")

    def test_pendulum_datetime_to_datetime_string(self):
        """pendulum DateTime.to_datetime_string() produces YYYY-MM-DD HH:MM:SS
        (replaces _to_eloqua_datetime for end_date DateTime inputs)."""
        dt = pendulum.parse("2024-08-15T12:30:45Z")
        result = dt.to_datetime_string()
        self.assertRegex(result, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
        self.assertEqual(result, "2024-08-15 12:30:45")

    @patch("tap_eloqua.sync.write_bookmark")
    @patch("tap_eloqua.sync.persist_records")
    @patch("tap_eloqua.sync.singer.write_schema")
    @patch("tap_eloqua.sync.pendulum.from_timestamp")
    @patch("tap_eloqua.sync.pendulum.parse")
    def test_sync_static_endpoint_invokes_pendulum_helpers(
        self,
        mock_parse,
        mock_from_timestamp,
        mock_write_schema,
        mock_persist_records,
        mock_write_bookmark,
    ):
        parse_dt = MagicMock()
        parse_dt.to_datetime_string.return_value = "2024-01-01 00:00:00"
        mock_parse.return_value = parse_dt

        from_ts_dt = MagicMock()
        from_ts_dt.to_iso8601_string.return_value = "2024-01-01T00:00:00Z"
        mock_from_timestamp.return_value = from_ts_dt

        catalog = Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": "visitors",
                        "stream": "visitors",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "createdAt": {"type": ["null", "string"]}
                            },
                        },
                        "key_properties": [],
                        "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
                    }
                ]
            }
        )
        client = MagicMock()
        client.get.return_value = {"elements": [{"createdAt": "1704067200"}]}

        sync_static_endpoint(
            client=client,
            catalog=catalog,
            state={},
            start_date="2024-01-01T00:00:00Z",
            stream_id="visitors",
            path="data/visitors",
            updated_at_col="createdAt",
        )

        mock_parse.assert_called_once_with("2024-01-01T00:00:00Z")
        mock_from_timestamp.assert_called_once_with(1704067200)
        mock_write_schema.assert_called_once()
        mock_persist_records.assert_called_once()
        mock_write_bookmark.assert_called_once_with({}, "visitors", "2024-01-01T00:00:00Z")

    @patch("tap_eloqua.sync.sync_bulk_obj")
    @patch("tap_eloqua.sync.update_current_stream")
    @patch("tap_eloqua.sync.pendulum.parse")
    @patch("tap_eloqua.sync.pendulum.now")
    def test_sync_activity_stream_invokes_pendulum_now_and_parse(
        self,
        mock_now,
        mock_parse,
        mock_update_current_stream,
        mock_sync_bulk_obj,
    ):
        sync_start = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        mock_now.return_value = sync_start
        mock_parse.side_effect = [
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ]

        sync_activity_stream(
            client=MagicMock(),
            stream_name="emails_sent",
            state={"bookmarks": {"emails_sent": {"datetime": "2024-01-01T00:00:00Z"}}},
            catalog=MagicMock(),
            start_date="2024-01-01T00:00:00Z",
            bulk_page_size=1000,
            activity_type="EmailSend",
        )

        mock_now.assert_called_once_with("UTC")
        self.assertEqual(mock_parse.call_count, 2)
        mock_parse.assert_any_call("2024-01-01T00:00:00Z")
        mock_update_current_stream.assert_called_once_with(
            {"bookmarks": {"emails_sent": {"datetime": "2024-01-01T00:00:00Z"}}},
            "emails_sent",
        )
        mock_sync_bulk_obj.assert_called_once()


if __name__ == "__main__":
    unittest.main()
