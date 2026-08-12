import unittest
from unittest.mock import MagicMock, patch

from singer import metadata as mdata

from tap_eloqua.discover import discover


class TestDiscoverUnit(unittest.TestCase):
    @patch("tap_eloqua.discover.get_pk")
    @patch("tap_eloqua.discover.get_schemas")
    def test_discover_sets_forced_replication_method_and_keys(self, mock_get_schemas, mock_get_pk):
        schemas = {
            "accounts": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "Id": {"type": "string"},
                    "UpdatedAt": {"type": "string", "format": "date-time"},
                },
            },
            "visitors": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "id": {"type": "string"},
                    "createdAt": {"type": "string"},
                },
            },
        }
        field_metadata = {
            "accounts": [
                {"breadcrumb": [], "metadata": {"tap-eloqua.query-language-name": "Account"}},
                {"breadcrumb": ["properties", "Id"], "metadata": {"inclusion": "automatic"}},
            ],
            "visitors": [
                {"breadcrumb": [], "metadata": {}},
                {"breadcrumb": ["properties", "id"], "metadata": {"inclusion": "automatic"}},
            ],
        }

        mock_get_schemas.return_value = (schemas, field_metadata)
        mock_get_pk.side_effect = lambda stream_name: ["Id"] if stream_name == "accounts" else []

        catalog = discover(MagicMock())
        stream_map = {stream.stream: stream for stream in catalog.streams}

        accounts_md = mdata.to_map(stream_map["accounts"].metadata)[()]
        visitors_md = mdata.to_map(stream_map["visitors"].metadata)[()]

        self.assertEqual(accounts_md.get("forced-replication-method"), "INCREMENTAL")
        self.assertEqual(visitors_md.get("forced-replication-method"), "FULL_TABLE")

        self.assertEqual(stream_map["accounts"].key_properties, ["Id"])
        self.assertEqual(stream_map["visitors"].key_properties, [])

    @patch("tap_eloqua.discover.get_pk", return_value=["Id"])
    @patch("tap_eloqua.discover.get_schemas")
    def test_discover_preserves_field_metadata(self, mock_get_schemas, mock_get_pk):
        schemas = {
            "accounts": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "Id": {"type": "string"},
                    "UpdatedAt": {"type": "string", "format": "date-time"},
                    "Name": {"type": ["null", "string"]},
                },
            }
        }
        field_metadata = {
            "accounts": [
                {"breadcrumb": [], "metadata": {"tap-eloqua.query-language-name": "Account"}},
                {
                    "breadcrumb": ["properties", "Name"],
                    "metadata": {
                        "inclusion": "available",
                        "tap-eloqua.statement": "{{Account.Name}}",
                    },
                },
            ]
        }

        mock_get_schemas.return_value = (schemas, field_metadata)

        catalog = discover(MagicMock())
        accounts = next(stream for stream in catalog.streams if stream.stream == "accounts")
        md_map = mdata.to_map(accounts.metadata)

        self.assertIn(("properties", "Name"), md_map)
        self.assertEqual(md_map[("properties", "Name")]["inclusion"], "available")
        self.assertEqual(
            md_map[("properties", "Name")]["tap-eloqua.statement"],
            "{{Account.Name}}",
        )


if __name__ == "__main__":
    unittest.main()
