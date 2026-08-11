import unittest
from itertools import count
from unittest.mock import MagicMock, patch

import pendulum
from requests.exceptions import HTTPError
from singer.catalog import Catalog

from tap_eloqua.sync import (
    ActivityExportTooLarge,
    persist_records,
    stream_export,
    sync,
    sync_activity_stream,
    sync_bulk_obj,
    transform_export_row,
)


class DummyResponse:
    def __init__(self, status_code):
        self.status_code = status_code


class TestSyncRuntimeUnit(unittest.TestCase):
    def _accounts_catalog(self):
        return Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": "accounts",
                        "stream": "accounts",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "Id": {"type": "string"},
                                "UpdatedAt": {"type": "string", "format": "date-time"},
                                "CreatedAt": {"type": ["null", "string"], "format": "date-time"},
                            },
                        },
                        "key_properties": ["Id"],
                        "metadata": [
                            {
                                "breadcrumb": [],
                                "metadata": {
                                    "selected": True,
                                    "tap-eloqua.id": None,
                                    "tap-eloqua.query-language-name": "Account",
                                },
                            },
                            {
                                "breadcrumb": ["properties", "Id"],
                                "metadata": {
                                    "inclusion": "automatic",
                                    "selected": False,
                                    "tap-eloqua.statement": "{{Account.Id}}",
                                },
                            },
                            {
                                "breadcrumb": ["properties", "UpdatedAt"],
                                "metadata": {
                                    "inclusion": "automatic",
                                    "selected": False,
                                    "tap-eloqua.statement": "{{Account.UpdatedAt}}",
                                },
                            },
                            {
                                "breadcrumb": ["properties", "CreatedAt"],
                                "metadata": {
                                    "inclusion": "automatic",
                                    "selected": False,
                                    "tap-eloqua.statement": "{{Account.CreatedAt}}",
                                },
                            },
                        ],
                    }
                ]
            }
        )

    def test_transform_export_row_converts_empty_strings(self):
        row = {"A": "", "B": "x"}
        transformed = transform_export_row(row)
        self.assertIsNone(transformed["A"])
        self.assertEqual(transformed["B"], "x")

    @patch("tap_eloqua.sync.singer.write_record")
    def test_persist_records_sets_created_at_for_activity(self, mock_write_record):
        records = [{"ActivityDate": "2024-01-01T00:00:00Z", "Id": "1"}]
        persist_records(self._accounts_catalog(), "accounts", records, activity_type="EmailOpen")
        written = mock_write_record.call_args.args[1]
        self.assertEqual(written["CreatedAt"], "2024-01-01T00:00:00.000000Z")

    @patch("tap_eloqua.sync.persist_records")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    @patch("tap_eloqua.sync.write_schema")
    def test_stream_export_handles_no_items_and_returns_bookmark(self, _mock_write_schema, mock_write_bulk_bookmark, _mock_persist_records):
        client = MagicMock()
        client.get.return_value = {"hasMore": False, "items": []}

        out = stream_export(
            client=client,
            state={},
            catalog=self._accounts_catalog(),
            stream_name="accounts",
            sync_id="10",
            updated_at_field="UpdatedAt",
            bulk_page_size=100,
            bookmark_datetime="2024-01-01T00:00:00Z",
        )

        self.assertEqual(out, "2024-01-01T00:00:00Z")
        self.assertGreaterEqual(mock_write_bulk_bookmark.call_count, 2)

    @patch("tap_eloqua.sync.persist_records")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    @patch("tap_eloqua.sync.write_schema")
    def test_stream_export_updates_max_updated_at_across_pages(self, _mock_write_schema, _mock_write_bulk_bookmark, _mock_persist_records):
        client = MagicMock()
        client.get.side_effect = [
            {"hasMore": True, "items": [{"UpdatedAt": "2024-01-01T00:00:00Z"}]},
            {"hasMore": False, "items": [{"UpdatedAt": "2024-01-02T00:00:00Z"}]},
        ]

        out = stream_export(
            client=client,
            state={},
            catalog=self._accounts_catalog(),
            stream_name="accounts",
            sync_id="10",
            updated_at_field="UpdatedAt",
            bulk_page_size=1,
            bookmark_datetime="2024-01-01T00:00:00Z",
        )

        self.assertEqual(out, "2024-01-02T00:00:00Z")

    @patch("tap_eloqua.sync.stream_export", return_value="2024-01-01 00:00:00")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_sync_bulk_obj_uses_custom_object_url_when_root_has_id(self, _mock_write_bulk_bookmark, _mock_stream_export):
        catalog = Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": "my_custom",
                        "stream": "my_custom",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "Id": {"type": "string"},
                                "UpdatedAt": {"type": "string", "format": "date-time"},
                            },
                        },
                        "key_properties": ["Id"],
                        "metadata": [
                            {"breadcrumb": [], "metadata": {"selected": True, "tap-eloqua.id": "321", "tap-eloqua.query-language-name": "CustomObject[321]"}},
                            {"breadcrumb": ["properties", "Id"], "metadata": {"inclusion": "automatic", "tap-eloqua.statement": "{{CustomObject[321].Id}}"}},
                            {"breadcrumb": ["properties", "UpdatedAt"], "metadata": {"inclusion": "automatic", "tap-eloqua.statement": "{{CustomObject[321].UpdatedAt}}"}},
                        ],
                    }
                ]
            }
        )
        client = MagicMock()
        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/99"}]
        client.get.side_effect = [
            {"status": "success"},
            {"items": [{"message": "Successfully exported members to csv file.", "count": 1}]},
        ]

        sync_bulk_obj(client, catalog, {}, "2024-01-01T00:00:00Z", "my_custom", 500)

        called_path = client.post.call_args_list[0].args[0]
        self.assertIn("customObjects/321/exports", called_path)

    @patch("tap_eloqua.sync.stream_export", return_value="2024-01-01 00:00:00")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_sync_bulk_obj_includes_activity_filter_and_removes_created_at_field(self, _mock_write_bulk_bookmark, _mock_stream_export):
        catalog = self._accounts_catalog()
        client = MagicMock()
        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/99"}]
        client.get.side_effect = [
            {"status": "success"},
            {"items": [{"message": "Successfully exported members to csv file.", "count": 1}]},
        ]

        sync_bulk_obj(client, catalog, {}, "2024-01-01T00:00:00Z", "accounts", 500, activity_type="EmailOpen")

        payload = client.post.call_args_list[0].kwargs["json"]
        self.assertIn("{{Activity.Type}}", payload["filter"])
        self.assertNotIn("CreatedAt", payload["fields"])

    @patch("tap_eloqua.sync.stream_export", return_value="2024-01-01 00:00:00")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_sync_bulk_obj_resumes_export_and_handles_404_expired(self, mock_write_bulk_bookmark, mock_stream_export):
        catalog = self._accounts_catalog()
        client = MagicMock()

        not_found = HTTPError("gone")
        not_found.response = DummyResponse(404)
        mock_stream_export.side_effect = [not_found, "2024-01-01 00:00:00"]

        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/99"}]
        client.get.side_effect = [
            {"status": "success"},
            {"items": [{"message": "Successfully exported members to csv file.", "count": 5}]},
        ]

        state = {"bookmarks": {"accounts": {"sync_id": "old", "offset": 0, "datetime": "2024-01-01T00:00:00Z"}}}

        sync_bulk_obj(client, catalog, state, "2024-01-01T00:00:00Z", "accounts", 500)

        self.assertGreaterEqual(mock_stream_export.call_count, 2)
        self.assertGreaterEqual(mock_write_bulk_bookmark.call_count, 1)

    @patch("tap_eloqua.sync.stream_export", return_value="2024-01-01 00:00:00")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_sync_bulk_obj_reraises_non_404_http_error(self, _mock_write_bulk_bookmark, mock_stream_export):
        catalog = self._accounts_catalog()
        client = MagicMock()

        generic_http_error = HTTPError("server-error")
        generic_http_error.response = DummyResponse(500)
        mock_stream_export.side_effect = generic_http_error

        state = {"bookmarks": {"accounts": {"sync_id": "old", "offset": 0, "datetime": "2024-01-01T00:00:00Z"}}}

        with self.assertRaises(HTTPError):
            sync_bulk_obj(client, catalog, state, "2024-01-01T00:00:00Z", "accounts", 500)

    @patch("tap_eloqua.sync.stream_export", return_value="2024-01-01 00:00:00")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_sync_bulk_obj_raises_on_failed_status(self, _mock_write_bulk_bookmark, _mock_stream_export):
        catalog = self._accounts_catalog()
        client = MagicMock()

        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/99"}]
        client.get.return_value = {"status": "error"}

        with self.assertRaises(Exception) as error_context:
            sync_bulk_obj(client, catalog, {}, "2024-01-01T00:00:00Z", "accounts", 500)
        self.assertIn("exporting failed", str(error_context.exception))

    @patch("tap_eloqua.sync.stream_export", return_value="2024-01-01 00:00:00")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    @patch("tap_eloqua.sync.time.sleep")
    @patch("tap_eloqua.sync.next_sleep_interval", return_value=2)
    @patch("tap_eloqua.sync.time.time")
    def test_sync_bulk_obj_pending_loops_and_sleeps(self, mock_time, _mock_next_sleep, mock_sleep, _mock_write_bulk_bookmark, _mock_stream_export):
        catalog = self._accounts_catalog()
        client = MagicMock()

        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/99"}]
        client.get.side_effect = [
            {"status": "pending"},
            {"status": "success"},
            {"items": [{"message": "Successfully exported members to csv file.", "count": 1}]},
        ]
        mock_time.side_effect = count(0)

        sync_bulk_obj(client, catalog, {}, "2024-01-01T00:00:00Z", "accounts", 500)
        mock_sleep.assert_called_once_with(2)

    @patch("tap_eloqua.sync.stream_export", return_value="2024-01-01 00:00:00")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    @patch("tap_eloqua.sync.time.time")
    def test_sync_bulk_obj_raises_on_deadline_exceeded(self, mock_time, _mock_write_bulk_bookmark, _mock_stream_export):
        catalog = self._accounts_catalog()
        client = MagicMock()

        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/99"}]
        client.get.return_value = {"status": "pending"}
        mock_time.side_effect = [0, 0, 21601]

        with self.assertRaises(Exception):
            sync_bulk_obj(client, catalog, {}, "2024-01-01T00:00:00Z", "accounts", 500)

    @patch("tap_eloqua.sync.stream_export", return_value="2024-01-01 00:00:00")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_sync_bulk_obj_activity_stream_too_large(self, _mock_write_bulk_bookmark, _mock_stream_export):
        catalog = self._accounts_catalog()
        client = MagicMock()

        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/99"}]
        client.get.side_effect = [
            {"status": "success"},
            {"items": [{"message": "Successfully exported members to csv file.", "count": 5000000}]},
        ]

        with self.assertRaises(ActivityExportTooLarge):
            sync_bulk_obj(client, catalog, {}, "2024-01-01T00:00:00Z", "accounts", 500, activity_type="EmailOpen")

    @patch("tap_eloqua.sync.stream_export", return_value="2024-01-01 00:00:00")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_sync_bulk_obj_includes_end_date_in_filter(self, _mock_write_bulk_bookmark, _mock_stream_export):
        catalog = self._accounts_catalog()
        client = MagicMock()

        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/99"}]
        client.get.side_effect = [
            {"status": "success"},
            {"items": [{"message": "Successfully exported members to csv file.", "count": 1}]},
        ]

        sync_bulk_obj(
            client,
            catalog,
            {},
            "2024-01-01T00:00:00Z",
            "accounts",
            500,
            end_date=pendulum.datetime(2024, 1, 2, 0, 0, 0, tz="UTC"),
        )

        payload = client.post.call_args_list[0].kwargs["json"]
        self.assertIn("< '2024-01-02 00:00:00'", payload["filter"])

    @patch("tap_eloqua.sync.sync_bulk_obj")
    @patch("tap_eloqua.sync.pendulum.now")
    @patch("tap_eloqua.sync.get_bulk_bookmark")
    def test_sync_activity_stream_retries_with_smaller_window(self, mock_get_bulk_bookmark, mock_now, mock_sync_bulk_obj):
        mock_now.return_value = pendulum.datetime(2024, 1, 1, 1, 0, 0, tz="UTC")
        mock_get_bulk_bookmark.return_value = {"datetime": "2024-01-01T00:00:00Z"}

        mock_sync_bulk_obj.side_effect = [ActivityExportTooLarge("too large"), None, None]

        with patch("tap_eloqua.sync.update_current_stream"):
            sync_activity_stream(
                client=MagicMock(),
                stream_name="activity_email_open",
                state={"bookmarks": {}},
                catalog=self._accounts_catalog(),
                start_date="2024-01-01T00:00:00Z",
                bulk_page_size=100,
                activity_type="EmailOpen",
            )

        self.assertGreaterEqual(mock_sync_bulk_obj.call_count, 2)

    @patch("tap_eloqua.sync.sync_bulk_obj")
    @patch("tap_eloqua.sync.pendulum.now")
    @patch("tap_eloqua.sync.get_bulk_bookmark")
    def test_sync_activity_stream_caps_end_date_to_sync_start(self, mock_get_bulk_bookmark, mock_now, mock_sync_bulk_obj):
        mock_now.return_value = pendulum.datetime(2024, 1, 1, 1, 0, 0, tz="UTC")
        mock_get_bulk_bookmark.return_value = {"datetime": "2024-01-01T02:00:00Z"}
        mock_sync_bulk_obj.side_effect = [ActivityExportTooLarge("too large"), None]

        with patch("tap_eloqua.sync.update_current_stream"):
            sync_activity_stream(
                client=MagicMock(),
                stream_name="activity_email_open",
                state={"bookmarks": {}},
                catalog=self._accounts_catalog(),
                start_date="2023-12-31T23:00:00Z",
                bulk_page_size=100,
                activity_type="EmailOpen",
            )

        second_call_end_date = mock_sync_bulk_obj.call_args_list[1].kwargs["end_date"]
        self.assertEqual(second_call_end_date, pendulum.datetime(2024, 1, 1, 1, 0, 0, tz="UTC"))

    @patch("tap_eloqua.sync.sync_static_endpoint")
    @patch("tap_eloqua.sync.sync_bulk_obj")
    @patch("tap_eloqua.sync.sync_activity_stream")
    @patch("tap_eloqua.sync.get_custom_obj_streams", return_value=[])
    @patch("tap_eloqua.sync.update_current_stream")
    @patch("tap_eloqua.sync.get_selected_streams", return_value=["accounts", "visitors"])
    def test_sync_orchestrator_invokes_selected_paths(
        self,
        _mock_get_selected,
        _mock_update_current,
        _mock_get_custom,
        mock_sync_activity_stream,
        mock_sync_bulk_obj,
        mock_sync_static_endpoint,
    ):
        sync(MagicMock(), self._accounts_catalog(), {}, "2024-01-01T00:00:00Z", 500)

        self.assertGreaterEqual(mock_sync_bulk_obj.call_count, 1)
        self.assertEqual(mock_sync_activity_stream.call_count, 0)
        self.assertGreaterEqual(mock_sync_static_endpoint.call_count, 1)

    @patch("tap_eloqua.sync.sync_static_endpoint")
    @patch("tap_eloqua.sync.sync_bulk_obj")
    @patch("tap_eloqua.sync.sync_activity_stream")
    @patch("tap_eloqua.sync.get_custom_obj_streams", return_value=["my_custom"])
    @patch("tap_eloqua.sync.update_current_stream")
    @patch("tap_eloqua.sync.get_selected_streams", return_value=["activity_email_open", "my_custom"])
    def test_sync_orchestrator_invokes_activity_and_custom_paths(
        self,
        _mock_get_selected,
        _mock_update_current,
        _mock_get_custom,
        mock_sync_activity_stream,
        mock_sync_bulk_obj,
        _mock_sync_static_endpoint,
    ):
        sync(MagicMock(), self._accounts_catalog(), {}, "2024-01-01T00:00:00Z", 500)

        self.assertGreaterEqual(mock_sync_activity_stream.call_count, 1)
        self.assertGreaterEqual(mock_sync_bulk_obj.call_count, 1)


if __name__ == "__main__":
    unittest.main()
