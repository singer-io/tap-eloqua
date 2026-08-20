import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog

from tap_eloqua.sync import sync_static_endpoint


class PaginationTest(unittest.TestCase):
    def _catalog(self):
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

    @patch("tap_eloqua.sync.persist_records")
    @patch("tap_eloqua.sync.write_bookmark")
    @patch("tap_eloqua.sync.write_schema")
    def test_static_endpoint_paginates_until_short_page(self, mock_write_schema, mock_write_bookmark, mock_persist_records):
        catalog = self._catalog()
        client = MagicMock()

        page_one = {
            "elements": [
                {"id": str(i), "V_LastVisitDateAndTime": 1704067200 + i} for i in range(1000)
            ]
        }
        page_two = {"elements": [{"id": "1001", "V_LastVisitDateAndTime": 1704069000}]}

        client.get.side_effect = [page_one, page_two]
        state = {}

        sync_static_endpoint(
            client,
            catalog,
            state,
            start_date="2024-01-01T00:00:00Z",
            stream_id="visitors",
            path="data/visitors",
            updated_at_col="V_LastVisitDateAndTime",
        )

        self.assertEqual(client.get.call_count, 2)
        first_params = client.get.call_args_list[0].kwargs["params"]
        second_params = client.get.call_args_list[1].kwargs["params"]
        self.assertEqual(first_params["page"], 1)
        self.assertEqual(second_params["page"], 2)

        self.assertEqual(mock_persist_records.call_count, 2)
        self.assertEqual(mock_write_bookmark.call_count, 2)


if __name__ == "__main__":
    unittest.main()
