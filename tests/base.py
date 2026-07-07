"""Base test class for tap-eloqua with common test utilities."""
import unittest
from unittest.mock import MagicMock

from singer.catalog import Catalog

from tap_eloqua.schema import get_schemas


class EloquaBaseTest(unittest.TestCase):
    """Base class for eloqua tapTests with common setup and utilities."""

    # Metadata keys for organization
    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"

    # Class-level schema cache shared across all subclasses
    _schemas_cache = None
    _field_metadata_cache = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if EloquaBaseTest._schemas_cache is None:
            EloquaBaseTest._schemas_cache, EloquaBaseTest._field_metadata_cache = get_schemas(
                MagicMock(get=lambda *a, **k: {"items": []})
            )

    @classmethod
    def expected_metadata(cls):
        """
        Return expected metadata for known eloqua streams.
        Maps stream name to expected properties.
        """
        return {
            # Built-in bulk objects (INCREMENTAL)
            "accounts": {
                cls.PRIMARY_KEYS: {"Id"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"UpdatedAt"},
                cls.OBEYS_START_DATE: True,
            },
            "contacts": {
                cls.PRIMARY_KEYS: {"Id"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"UpdatedAt"},
                cls.OBEYS_START_DATE: True,
            },
            # Activity streams (FULL_TABLE - no primary key)
            "activity_email_open": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "activity_email_clickthrough": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "activity_email_send": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "activity_form_submit": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "activity_subscribe": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "activity_unsubscribe": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "activity_bounceback": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "activity_web_visit": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "activity_page_view": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            # Static REST endpoints
            "visitors": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: True,
            },
            "campaigns": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"updatedAt"},
                cls.OBEYS_START_DATE: True,
            },
            "emails": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"updatedAt"},
                cls.OBEYS_START_DATE: True,
            },
            "forms": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"updatedAt"},
                cls.OBEYS_START_DATE: True,
            },
            "assets": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"updatedAt"},
                cls.OBEYS_START_DATE: True,
            },
            "emailGroups": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"updatedAt"},
                cls.OBEYS_START_DATE: True,
            },
        }

    @staticmethod
    def _schema_type(schema):
        """Return concrete type when schema allows null union types."""
        schema_type = schema.get("type", "object")
        if isinstance(schema_type, list):
            # Handle ["null", "string"] type definitions
            non_null = [item for item in schema_type if item != "null"]
            return non_null[0] if non_null else "null"
        return schema_type

    @staticmethod
    def _generate_value(schema, date_value="2024-01-01T00:00:00Z"):
        """Generate one valid mock value for a JSON-schema fragment."""
        if "enum" in schema and schema["enum"]:
            return schema["enum"][0]

        schema_type = EloquaBaseTest._schema_type(schema)

        if schema_type == "object":
            properties = schema.get("properties", {})
            required = set(schema.get("required", []))
            return {
                key: EloquaBaseTest._generate_value(value, date_value=date_value)
                for key, value in properties.items()
                if key in required or EloquaBaseTest._schema_type(value) != "null"
            }

        if schema_type == "array":
            return [
                EloquaBaseTest._generate_value(
                    schema.get("items", {"type": "string"}),
                    date_value=date_value,
                )
            ]

        if schema_type == "string":
            fmt = schema.get("format")
            return (
                date_value if fmt == "date-time" else "mock_string"
            )

        if schema_type == "integer":
            return 1

        if schema_type == "number":
            return 1.0

        if schema_type == "boolean":
            return True

        return None

    @classmethod
    def _generate_stream_record(cls, stream_name, date_value="2024-01-01T00:00:00Z"):
        """Generate one schema-valid record for a stream."""
        schemas = EloquaBaseTest._schemas_cache
        if schemas is None or stream_name not in schemas:
            raise ValueError(f"Stream {stream_name} not found in schemas")
        return cls._generate_value(schemas[stream_name], date_value=date_value)

    @classmethod
    def _accounts_catalog(cls, name_selected=True, extra_properties=None, extra_metadata=None):
        """
        Build a minimal accounts Catalog for use in tests.

        Args:
            name_selected: Whether the 'Name' available field is marked selected.
            extra_properties: Optional dict of additional property definitions.
            extra_metadata: Optional list of additional metadata breadcrumb entries.
        """
        properties = {
            "Id": {"type": "string"},
            "UpdatedAt": {"type": "string", "format": "date-time"},
            "Name": {"type": ["null", "string"]},
            "Email": {"type": ["null", "string"]},
        }
        md = [
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
                    "selected": name_selected,
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
        ]
        if extra_properties:
            properties.update(extra_properties)
        if extra_metadata:
            md.extend(extra_metadata)
        return Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": "accounts",
                        "stream": "accounts",
                        "schema": {"type": "object", "properties": properties},
                        "key_properties": ["Id"],
                        "metadata": md,
                    }
                ]
            }
        )


if __name__ == "__main__":
    unittest.main()
