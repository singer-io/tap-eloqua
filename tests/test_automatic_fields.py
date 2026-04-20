import unittest
from unittest.mock import MagicMock, patch

from tap_eloqua.sync import sync_bulk_obj

from .base import EloquaBaseTest


class AutomaticFieldsTest(EloquaBaseTest):
    def _catalog(self):
        return self._accounts_catalog(name_selected=False)

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
