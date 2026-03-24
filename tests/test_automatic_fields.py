import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog

from tap_eloqua.sync import sync_bulk_obj


class AutomaticFieldsTest(unittest.TestCase):
    def _catalog(self):
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
                                "Name": {"type": ["null", "string"]},
                                "Email": {"type": ["null", "string"]},
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
                                "breadcrumb": ["properties", "Name"],
                                "metadata": {
                                    "inclusion": "available",
                                    "selected": False,
                                    "tap-eloqua.statement": "{{Account.Name}}",
                                },
                            },
                            {
                                "breadcrumb": ["properties", "Email"],
                                "metadata": {
                                    "inclusion": "available",
                                    "selected": True,
                                    "tap-eloqua.statement": "{{Account.Email}}",
                                },
                            },
                        ],
                    }
                ]
            }
        )

    @patch("tap_eloqua.sync.stream_export")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_bulk_sync_includes_automatic_even_when_unselected(self, mock_write_bulk_bookmark, mock_stream_export):
        catalog = self._catalog()
        client = MagicMock()

        client.post.side_effect = [{"uri": "/exports/1"}, {"uri": "/syncs/7"}]
        client.get.side_effect = [
            {"status": "success"},
            {"items": [{"message": "Successfully exported members to csv file.", "count": 2}]},
        ]

        sync_bulk_obj(
            client,
            catalog,
            state={},
            start_date="2024-01-01T00:00:00Z",
            stream_name="accounts",
            bulk_page_size=500,
        )

        export_payload = client.post.call_args_list[0].kwargs["json"]
        field_map = export_payload["fields"]

        self.assertIn("Id", field_map)
        self.assertIn("UpdatedAt", field_map)
        self.assertIn("Email", field_map)
        self.assertNotIn("Name", field_map)


if __name__ == "__main__":
    unittest.main()
