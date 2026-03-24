"""Enhanced discovery tests for tap-eloqua following Singer standards."""
import json
import unittest
from unittest.mock import MagicMock

from singer import metadata

from tap_eloqua.discover import discover

try:
    from .base import EloquaBaseTest
except ImportError:
    from base import EloquaBaseTest


class DiscoveryTest(EloquaBaseTest):
    """Tests for discovery mode - catalog generation and metadata."""

    def _create_mock_client(self):
        """Create a mock Eloqua API client for testing."""
        mock_client = MagicMock()

        # Mock custom objects endpoint
        mock_client.get.side_effect = lambda path, params=None, endpoint=None: (
            {"items": []}
            if path == "/api/bulk/2.0/customObjects"
            else (
                # Activity/bulk fields
                {
                    "items": [
                        {
                            "internalName": "TestField",
                            "dataType": "string",
                            "statement": "{{TestField}}",
                            "uri": "/fields/1",
                        }
                    ]
                }
                if "fields" in path
                else {"items": []}
            )
        )
        return mock_client

    def test_discovery_returns_catalog(self):
        """Test that discover() returns a valid catalog object."""
        client = self._create_mock_client()
        catalog = discover(client)

        self.assertIsNotNone(catalog)
        self.assertTrue(hasattr(catalog, "streams"))
        self.assertGreater(len(catalog.streams), 0)

    def test_discovery_includes_all_stream_types(self):
        """
        Test that discovery includes:
        1. Built-in bulk objects (accounts, contacts)
        2. Activity streams
        3. Static schema streams
        """
        client = self._create_mock_client()
        catalog = discover(client)
        stream_names = {stream.stream for stream in catalog.streams}

        # Verify presence of key stream types
        self.assertIn("accounts", stream_names)
        self.assertIn("contacts", stream_names)
        self.assertIn("activity_email_open", stream_names)
        self.assertIn("visitors", stream_names)
        self.assertIn("campaigns", stream_names)

    def test_stream_has_required_fields(self):
        """Test that each stream entry has required Singer fields."""
        client = self._create_mock_client()
        catalog = discover(client)

        required_stream_fields = {"stream", "tap_stream_id", "schema", "key_properties"}

        for stream in catalog.streams:
            for field in required_stream_fields:
                self.assertTrue(
                    hasattr(stream, field),
                    f"Stream '{stream.stream}' missing required field '{field}'",
                )

    def test_stream_metadata_format(self):
        """Test that stream metadata follows Singer breadcrumb format."""
        client = self._create_mock_client()
        catalog = discover(client)

        for stream in catalog.streams:
            md_map = metadata.to_map(stream.metadata)

            # Check for root metadata (empty breadcrumb)
            self.assertIn(
                (),
                md_map,
                f"Stream '{stream.stream}' missing root metadata (empty breadcrumb)",
            )

            # Check that breadcrumbs are tuples
            for breadcrumb in md_map.keys():
                self.assertIsInstance(
                    breadcrumb, tuple, f"Breadcrumb {breadcrumb} is not a tuple"
                )

    def test_known_streams_have_forced_replication_method(self):
        """Test that known streams have forced-replication-method metadata."""
        client = self._create_mock_client()
        catalog = discover(client)
        expected = self.expected_metadata()

        for stream in catalog.streams:
            if stream.stream in expected:
                md_map = metadata.to_map(stream.metadata)
                root_md = md_map[()]

                expected_method = expected[stream.stream][self.REPLICATION_METHOD]
                actual_method = root_md.get("forced-replication-method")

                self.assertEqual(
                    actual_method,
                    expected_method,
                    f"Stream '{stream.stream}' replication method mismatch",
                )

    def test_bulk_streams_have_eloqua_metadata(self):
        """Test that bulk streams have Eloqua-specific metadata."""
        client = self._create_mock_client()
        catalog = discover(client)

        bulk_streams = {"accounts", "contacts"}

        for stream in catalog.streams:
            if stream.stream in bulk_streams:
                md_map = metadata.to_map(stream.metadata)
                root_md = md_map[()]

                self.assertIn(
                    "tap-eloqua.query-language-name",
                    root_md,
                    f"Stream '{stream.stream}' missing tap-eloqua.query-language-name",
                )

    def test_static_streams_from_schema_files(self):
        """Test that static schema streams are loaded correctly."""
        client = self._create_mock_client()
        catalog = discover(client)
        stream_names = {stream.stream for stream in catalog.streams}

        # These should come from /schemas/*.json files
        expected_static = {"assets", "campaigns", "emails", "forms", "emailGroups", "visitors"}
        for static_stream in expected_static:
            self.assertIn(
                static_stream,
                stream_names,
                f"Static stream '{static_stream}' not found in discovery",
            )

    def test_stream_schemas_are_valid_json_schema(self):
        """Test that all stream schemas are valid JSON Schema objects."""
        client = self._create_mock_client()
        catalog = discover(client)

        for stream in catalog.streams:
            schema = stream.schema.to_dict()

            # Basic JSON Schema structure
            self.assertIsInstance(schema, dict)
            self.assertIn("type", schema)
            self.assertEqual(schema["type"], "object")

            # Properties should be a dict
            if "properties" in schema:
                self.assertIsInstance(schema["properties"], dict)

    def test_key_properties_are_lists(self):
        """Test that key_properties are properly formatted as lists."""
        client = self._create_mock_client()
        catalog = discover(client)

        for stream in catalog.streams:
            self.assertIsInstance(
                stream.key_properties,
                list,
                f"Stream '{stream.stream}' key_properties is not a list",
            )

            # Check that all key properties exist in schema
            schema = stream.schema.to_dict()
            for key in stream.key_properties:
                self.assertIn(
                    key,
                    schema.get("properties", {}),
                    f"Key property '{key}' not found in schema for stream '{stream.stream}'",
                )

    def test_field_level_metadata_inclusion(self):
        """Test that fields have proper inclusion metadata."""
        client = self._create_mock_client()
        catalog = discover(client)

        valid_inclusions = {"automatic", "available", "unsupported"}

        for stream in catalog.streams:
            for md in stream.metadata:
                breadcrumb = md.get("breadcrumb", [])
                # Skip root metadata (which may not have inclusion)
                if breadcrumb and len(breadcrumb) >= 2:
                    inclusion = md["metadata"].get("inclusion")
                    self.assertIn(
                        inclusion,
                        valid_inclusions,
                        f"Invalid inclusion '{inclusion}' in stream '{stream.stream}'",
                    )

    def test_catalog_serialization(self):
        """Test that catalog can be serialized to JSON."""
        client = self._create_mock_client()
        catalog = discover(client)
        catalog_dict = catalog.to_dict()

        # Should serialize properly
        catalog_json = json.dumps(catalog_dict)
        self.assertIsNotNone(catalog_json)

        # Should be deserializable
        deserialized = json.loads(catalog_json)
        self.assertIn("streams", deserialized)
        self.assertEqual(len(deserialized["streams"]), len(catalog.streams))


if __name__ == "__main__":
    unittest.main()
