import unittest
from unittest.mock import MagicMock, patch

from requests import Response
from requests.exceptions import HTTPError

import tap_eloqua.schema as schema_module


def _make_http_error(status_code):
    response = MagicMock(spec=Response)
    response.status_code = status_code
    return HTTPError(response=response)


class TestSchemaDiscoveryAuthUnit(unittest.TestCase):
    def setUp(self):
        schema_module.SCHEMAS = None
        schema_module.FIELD_METADATA = None

    def tearDown(self):
        schema_module.SCHEMAS = None
        schema_module.FIELD_METADATA = None

    @patch("tap_eloqua.schema.get_static_schemas")
    @patch("tap_eloqua.schema.get_bulk_obj_schema")
    def test_get_schemas_skips_unauthorized_dynamic_streams(self, mock_get_bulk_obj_schema, _mock_static):
        def _side_effect(_client, stream_name, _obj_name, _system_fields, **_kwargs):
            if stream_name == "accounts":
                raise _make_http_error(403)
            return ({"type": "object", "properties": {}}, [])

        mock_get_bulk_obj_schema.side_effect = _side_effect

        client = MagicMock()
        client.get.return_value = {"items": []}

        schemas, field_metadata = schema_module.get_schemas(client)

        self.assertNotIn("accounts", schemas)
        self.assertNotIn("accounts", field_metadata)
        self.assertIn("contacts", schemas)
        self.assertIn("activity_email_open", schemas)

    @patch("tap_eloqua.schema.get_static_schemas")
    @patch("tap_eloqua.schema.get_bulk_obj_schema", return_value=({"type": "object", "properties": {}}, []))
    def test_get_schemas_skips_custom_objects_on_unauthorized_listing(self, _mock_bulk, _mock_static):
        client = MagicMock()

        def _client_get(path, **_kwargs):
            if path == "/api/bulk/2.0/customObjects":
                raise _make_http_error(401)
            return {"items": []}

        client.get.side_effect = _client_get

        schemas, _field_metadata = schema_module.get_schemas(client)

        self.assertIn("contacts", schemas)
        self.assertIn("activity_email_open", schemas)


if __name__ == "__main__":
    unittest.main()
