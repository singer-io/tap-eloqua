import unittest

from singer.catalog import Catalog

from tap_eloqua.sync import (
    get_selected_streams,
    get_custom_obj_streams,
    should_sync_stream,
    transform_export_row,
)


class TestSyncSelectionUnit(unittest.TestCase):
    def _catalog(self):
        return Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": "accounts",
                        "stream": "accounts",
                        "schema": {"type": "object", "properties": {"Id": {"type": "string"}}},
                        "key_properties": ["Id"],
                        "metadata": [
                            {
                                "breadcrumb": [],
                                "metadata": {
                                    "selected": True,
                                    "tap-eloqua.id": None,
                                },
                            }
                        ],
                    },
                    {
                        "tap_stream_id": "custom_lead_score",
                        "stream": "custom_lead_score",
                        "schema": {"type": "object", "properties": {"Id": {"type": "string"}}},
                        "key_properties": ["Id"],
                        "metadata": [
                            {
                                "breadcrumb": [],
                                "metadata": {
                                    "selected": False,
                                    "tap-eloqua.id": "42",
                                },
                            }
                        ],
                    },
                    {
                        "tap_stream_id": "visitors",
                        "stream": "visitors",
                        "schema": {"type": "object", "properties": {"id": {"type": "string"}}},
                        "key_properties": [],
                        "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
                    },
                ]
            }
        )

    def test_get_selected_streams(self):
        selected = set(get_selected_streams(self._catalog()))
        self.assertEqual(selected, {"accounts", "visitors"})

    def test_get_custom_obj_streams(self):
        custom = set(get_custom_obj_streams(self._catalog()))
        self.assertEqual(custom, {"custom_lead_score"})

    def test_should_sync_stream_when_resuming(self):
        should_stream, last_stream = should_sync_stream(
            selected_streams=["accounts", "visitors"],
            last_stream="accounts",
            stream_name="accounts",
        )
        self.assertTrue(should_stream)
        self.assertIsNone(last_stream)

    def test_should_not_sync_stream_if_not_selected(self):
        should_stream, last_stream = should_sync_stream(
            selected_streams=["accounts"],
            last_stream=None,
            stream_name="visitors",
        )
        self.assertFalse(should_stream)
        self.assertIsNone(last_stream)

    def test_transform_export_row_empty_string_to_null(self):
        row = {"Id": "1", "Name": "", "Title": "Engineer"}
        out = transform_export_row(row)
        self.assertEqual(out["Id"], "1")
        self.assertIsNone(out["Name"])
        self.assertEqual(out["Title"], "Engineer")


if __name__ == "__main__":
    unittest.main()
