import unittest
from unittest.mock import MagicMock, patch

from tap_eloqua.sync import sync_bulk_obj

from .base import EloquaBaseTest


class AllFieldsTest(EloquaBaseTest):
    def _catalog(self, include_many=False):
        extra_properties = {}
        extra_metadata = []
        if include_many:
            for index in range(251):
                field_name = f"Field{index}"
                extra_properties[field_name] = {"type": ["null", "string"]}
                extra_metadata.append(
                    {
                        "breadcrumb": ["properties", field_name],
                        "metadata": {
                            "inclusion": "available",
                            "selected": True,
                            "tap-eloqua.statement": "{{Account." + field_name + "}}",
                        },
                    }
                )
        return self._accounts_catalog(
            name_selected=True,
            extra_properties=extra_properties or None,
            extra_metadata=extra_metadata or None,
        )

    @patch("tap_eloqua.sync.stream_export")
    @patch("tap_eloqua.sync.write_bulk_bookmark")
    def test_bulk_sync_includes_all_selected_and_automatic_fields(self, mock_write_bulk_bookmark, mock_stream_export):
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
        self.assertIn("Name", field_map)
        self.assertIn("Email", field_map)

    def test_bulk_sync_raises_if_selected_fields_exceed_limit(self):
        catalog = self._catalog(include_many=True)
        client = MagicMock()

        with self.assertRaises(Exception) as ctx:
            sync_bulk_obj(
                client,
                catalog,
                state={},
                start_date="2024-01-01T00:00:00Z",
                stream_name="accounts",
                bulk_page_size=500,
            )

        self.assertIn("Exports can only have 250 fields selected", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
