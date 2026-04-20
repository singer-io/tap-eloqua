import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog

from tap_eloqua.sync import sync_static_endpoint, sync_bulk_obj


class StartDateTest(unittest.TestCase):
    def _static_catalog(self):
        return Catalog.from_dict(
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

    def _bulk_catalog(self):
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
                                    "tap-eloqua.statement": "{{Account.Id}}",
                                },
                            },
                            {
                                "breadcrumb": ["properties", "UpdatedAt"],
                                "metadata": {
                                    "inclusion": "automatic",
                                    "tap-eloqua.statement": "{{Account.UpdatedAt}}",
                                },
                            },
                        ],
                    }
                ]
            }
        )

    @patch("tap_eloqua.sync.persist_records")
    @patch("tap_eloqua.sync.write_schema")
    def test_static_stream_uses_start_date_when_no_bookmark(self, mock_write_schema, mock_persist_records):
        catalog = self._static_catalog()
        client = MagicMock()
        client.get.return_value = {"elements": []}

        sync_static_endpoint(
            client,
            catalog,
            state={},
            start_date="2024-06-01T00:00:00Z",
            stream_id="visitors",
            path="data/visitors",
            updated_at_col="V_LastVisitDateAndTime",
        )

        sent_params = client.get.call_args.kwargs["params"]
        self.assertIn("2024-06-01 00:00:00", sent_params["search"])

    @patch("tap_eloqua.sync.stream_export")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_bulk_stream_uses_bookmark_over_start_date(self, mock_write_bulk_bookmark, mock_stream_export):
        catalog = self._bulk_catalog()
        client = MagicMock()

        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/99"}]
        client.get.side_effect = [
            {"status": "success"},
            {"items": [{"message": "Successfully exported members to csv file.", "count": 1}]},
        ]

        state = {"bookmarks": {"accounts": {"datetime": "2024-08-01T00:00:00Z"}}}

        sync_bulk_obj(
            client,
            catalog,
            state=state,
            start_date="2024-01-01T00:00:00Z",
            stream_name="accounts",
            bulk_page_size=500,
        )

        export_payload = client.post.call_args_list[0].kwargs["json"]
        self.assertIn("2024-08-01 00:00:00", export_payload["filter"])


if __name__ == "__main__":
    unittest.main()
