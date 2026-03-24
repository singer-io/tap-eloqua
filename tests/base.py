"""Base test class for tap-eloqua with common test utilities."""
import unittest
from unittest.mock import patch, MagicMock

from tap_eloqua.schema import get_schemas


class EloquaBaseTest(unittest.TestCase):
    """Base class for eloqua tapTests with common setup and utilities."""

    # Metadata keys for organization
    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"

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
            # Static REST endpoints
            "visitors": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: {"V_LastVisitDateAndTime"},
                cls.OBEYS_START_DATE: False,
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
        schemas, _ = get_schemas(MagicMock(get=lambda *a, **k: {"items": []}))
        if stream_name not in schemas:
            raise ValueError(f"Stream {stream_name} not found in schemas")
        return cls._generate_value(schemas[stream_name], date_value=date_value)


if __name__ == "__main__":
    unittest.main()
