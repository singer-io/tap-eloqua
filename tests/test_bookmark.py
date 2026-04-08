import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog

from tap_eloqua.sync import write_bookmark, write_bulk_bookmark, sync_static_endpoint


class BookmarkTest(unittest.TestCase):
    @patch("tap_eloqua.sync.singer.write_state")
    def test_write_bookmark_updates_state_and_emits(self, mock_write_state):
        state = {}
        write_bookmark(state, "visitors", "2024-01-01T00:00:00Z")

        # Validate overall bookmark state structure
        self.assertIsInstance(state, dict)
        self.assertIn("bookmarks", state)
        self.assertIsInstance(state["bookmarks"], dict)

        self.assertEqual(state["bookmarks"]["visitors"], "2024-01-01T00:00:00Z")
        mock_write_state.assert_called_once_with(state)

    @patch("tap_eloqua.sync.singer.write_state")
    def test_write_bulk_bookmark_updates_state_and_emits(self, mock_write_state):
        state = {}
        write_bulk_bookmark(
            state,
            stream_name="accounts",
            sync_id="12",
            offset=200,
            max_updated_at="2024-01-02T00:00:00Z",
        )

        self.assertEqual(
            state["bookmarks"]["accounts"],
            {
                "sync_id": "12",
                "offset": 200,
                "datetime": "2024-01-02T00:00:00Z",
            },
        )
        mock_write_state.assert_called_once_with(state)

    @patch("tap_eloqua.sync.persist_records")
    @patch("tap_eloqua.sync.write_schema")
    def test_static_sync_uses_bookmark_in_search(self, mock_write_schema, mock_persist_records):
        catalog = Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": "visitors",
                        "stream": "visitors",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "V_LastVisitDateAndTime": {"type": "string", "format": "date-time"},
                            },
                        },
                        "key_properties": [],
                        "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
                    }
                ]
            }
        )

        client = MagicMock()
        client.get.return_value = {"elements": []}

        state = {"bookmarks": {"visitors": "2024-09-10T10:11:12Z"}}
        sync_static_endpoint(
            client,
            catalog,
            state,
            start_date="2024-01-01T00:00:00Z",
            stream_id="visitors",
            path="data/visitors",
            updated_at_col="V_LastVisitDateAndTime",
        )

        sent_params = client.get.call_args.kwargs["params"]
        self.assertIn("2024-09-10 10:11:12", sent_params["search"])


if __name__ == "__main__":
    unittest.main()
