import unittest
from unittest.mock import MagicMock, patch

from tap_eloqua import schema as schema_module


class TestSchemaRuntimeUnit(unittest.TestCase):
    def tearDown(self):
        schema_module.SCHEMAS = None
        schema_module.FIELD_METADATA = None

    def test_get_bulk_schema_adds_field_id_from_uri_and_activity_language(self):
        client = MagicMock()
        client.get.return_value = {
            "items": [
                {
                    "internalName": "CustomField",
                    "dataType": "date",
                    "statement": "{{Activity.CustomField}}",
                    "uri": "/api/bulk/2.0/contacts/fields/42",
                }
            ]
        }

        schema, metadata = schema_module.get_bulk_schema(
            client,
            stream_name="activity_email_open",
            path="/api/bulk/2.0/activities/fields",
            system_fields=schema_module.ACTIVITY_BASE_SYSTEM_FIELD,
            activity_type="EmailOpen",
        )

        self.assertIn("CustomField", schema["properties"])
        self.assertEqual(schema["properties"]["CustomField"]["format"], "date-time")
        root_meta = next(item for item in metadata if item["breadcrumb"] == [])
        self.assertEqual(root_meta["metadata"]["tap-eloqua.query-language-name"], "Activity")

        field_meta = next(item for item in metadata if item["breadcrumb"] == ["properties", "CustomField"])
        self.assertEqual(field_meta["metadata"]["tap-eloqua.id"], "42")

    def test_get_bulk_schema_raises_on_duplicate_non_system_field(self):
        client = MagicMock()
        client.get.return_value = {
            "items": [
                {
                    "internalName": "ExternalOnly",
                    "dataType": "string",
                    "statement": "{{Contact.ExternalOnly}}",
                },
                {
                    "internalName": "ExternalOnly",
                    "dataType": "string",
                    "statement": "{{Contact.ExternalOnly}}",
                },
            ]
        }

        with self.assertRaises(Exception) as error_context:
            schema_module.get_bulk_schema(
                client,
                stream_name="accounts",
                path="/api/bulk/2.0/accounts/fields",
                system_fields={},
            )
        self.assertIn("Duplicate field detected", str(error_context.exception))

    def test_get_bulk_schema_skips_duplicate_system_field(self):
        client = MagicMock()
        client.get.return_value = {
            "items": [
                {
                    "internalName": "Id",
                    "dataType": "string",
                    "statement": "{{Account.Id}}",
                }
            ]
        }

        schema, metadata = schema_module.get_bulk_schema(
            client,
            stream_name="accounts",
            path="/api/bulk/2.0/accounts/fields",
            system_fields={"Id": {"type": ["null", "string"]}},
            query_language_name="Account",
        )

        self.assertIn("Id", schema["properties"])
        self.assertTrue(any(item["breadcrumb"] == ["properties", "Id"] for item in metadata))

    @patch("tap_eloqua.schema.get_static_schemas")
    @patch("tap_eloqua.schema.get_bulk_schema")
    @patch("tap_eloqua.schema.get_bulk_obj_schema")
    def test_get_schemas_builds_bulk_activity_and_custom_objects(
        self, mock_get_bulk_obj_schema, mock_get_bulk_schema, mock_get_static_schemas
    ):
        schema_module.SCHEMAS = None
        schema_module.FIELD_METADATA = None

        client = MagicMock()
        client.get.return_value = {
            "items": [
                {
                    "uri": "/customObjects/99",
                    "name": "Order Events",
                }
            ]
        }

        mock_get_bulk_obj_schema.return_value = (
            {"type": "object", "properties": {"Id": {"type": ["null", "string"]}}},
            [{"breadcrumb": [], "metadata": {"tap-eloqua.query-language-name": "Account"}}],
        )
        mock_get_bulk_schema.return_value = (
            {"type": "object", "properties": {"Id": {"type": ["null", "string"]}}},
            [{"breadcrumb": [], "metadata": {"tap-eloqua.query-language-name": "CustomObject[99]"}}],
        )

        schemas, metadata = schema_module.get_schemas(client)

        self.assertIn("accounts", schemas)
        self.assertIn("contacts", schemas)
        self.assertIn("order_events", schemas)
        self.assertTrue(any(name.startswith("activity_") for name in schemas.keys()))
        self.assertIn("order_events", metadata)
        mock_get_static_schemas.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
