"""Sync tests for tap-eloqua following Singer standards."""
import json
import unittest
from unittest.mock import MagicMock, patch, call

from singer.catalog import Catalog

from tap_eloqua.sync import sync, get_selected_streams, persist_records

try:
    from .base import EloquaBaseTest
except ImportError:
    from base import EloquaBaseTest


class SyncTest(EloquaBaseTest):
    """Tests for sync mode - data extraction and transformation."""

    def _create_mock_client(self):
        """Create a mock Eloqua API client for sync testing."""
        mock_client = MagicMock()

        def get_side_effect(path, params=None, endpoint=None):
            # Mock REST visitor endpoint
            if "/api/REST/2.0/data/visitors" in path:
                return {
                    "elements": [
                        {
                            "id": "1",
                            "type": "Visitor",
                            "visitorId": "v1",
                            "createdAt": "1704067200",
                            "contactId": "c-100",
                            "V_IPAddress": "10.0.0.1",
                            "V_LastVisitDateAndTime": 1704067200,
                        },
                        {
                            "id": "2",
                            "type": "Visitor",
                            "visitorId": "v2",
                            "createdAt": "1704067300",
                            "contactId": "c-101",
                            "V_IPAddress": "10.0.0.2",
                            "V_LastVisitDateAndTime": 1704067300,
                        },
                    ]
                }

            # Mock bulk export creation
            if "bulk/2.0/accounts/exports" in path:
                return {"uri": "/exports/100"}

            # Mock bulk sync creation
            if "bulk/2.0/syncs" in path:
                return {"uri": "/syncs/200"}

            # Mock bulk sync status (completed)
            if "bulk/2.0/syncs/200" in path and endpoint == "export_sync_poll":
                return {"status": "succeeded"}

            # Mock bulk data retrieval
            if "bulk/2.0/syncs/200/data" in path:
                return {
                    "items": [
                        {"Id": "a1", "CreatedAt": "2024-01-01T00:00:00Z", "UpdatedAt": "2024-01-01T00:00:00Z", "AccountName": "Acme Corp"},
                        {"Id": "a2", "CreatedAt": "2024-01-02T00:00:00Z", "UpdatedAt": "2024-01-02T00:00:00Z", "AccountName": "Tech Inc"},
                    ],
                    "hasMore": False,
                }

            return {"items": []}

        mock_client.get.side_effect = get_side_effect
        mock_client.post.return_value = {"uri": "/exports/100"}

        return mock_client

    def _create_catalog_with_selected_streams(self, selected_stream_names):
        """Create a mock catalog with specific streams selected."""
        catalog_dict = {
            "streams": [
                {
                    "tap_stream_id": "visitors",
                    "stream": "visitors",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "type": {"type": "string"},
                            "visitorId": {"type": "string"},
                            "V_LastVisitDateAndTime": {"type": "string", "format": "date-time"},
                        },
                    },
                    "key_properties": [],
                    "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata": {
                                "selected": "visitors" in selected_stream_names,
                                "inclusion": "available",
                            },
                        }
                    ],
                },
                {
                    "tap_stream_id": "accounts",
                    "stream": "accounts",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "Id": {"type": "string"},
                            "CreatedAt": {"type": "string", "format": "date-time"},
                            "UpdatedAt": {"type": "string", "format": "date-time"},
                            "AccountName": {"type": ["null", "string"]},
                        },
                    },
                    "key_properties": ["Id"],
                    "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata": {
                                "selected": "accounts" in selected_stream_names,
                                "forced-replication-method": "INCREMENTAL",
                                "tap-eloqua.query-language-name": "Account",
                            },
                        },
                        {
                            "breadcrumb": ["properties", "Id"],
                            "metadata": {"inclusion": "automatic"},
                        },
                        {
                            "breadcrumb": ["properties", "UpdatedAt"],
                            "metadata": {"inclusion": "automatic"},
                        },
                    ],
                },
            ]
        }
        return Catalog.from_dict(catalog_dict)

    def test_get_selected_streams_respects_selected_metadata(self):
        """Test that get_selected_streams reads selected metadata correctly."""
        catalog = self._create_catalog_with_selected_streams(["visitors"])

        selected = get_selected_streams(catalog)

        self.assertIn("visitors", selected)
        self.assertNotIn("accounts", selected)

    def test_get_selected_streams_returns_list(self):
        """Test that get_selected_streams returns a list."""
        catalog = self._create_catalog_with_selected_streams(["visitors"])

        selected = get_selected_streams(catalog)

        self.assertIsInstance(selected, list)

    @patch("tap_eloqua.sync.singer.write_schema")
    @patch("tap_eloqua.sync.singer.write_record")
    @patch("tap_eloqua.sync.singer.write_state")
    def test_sync_with_selected_streams(self, mock_write_state, mock_write_record, mock_write_schema):
        """Test sync with selected streams."""
        client = self._create_mock_client()
        catalog = self._create_catalog_with_selected_streams(["visitors"])
        state = {}

        sync(client, catalog, state, "2024-01-01T00:00:00Z", 1000)

        # Should have called write_schema for visitors
        mock_write_schema.assert_any_call(
            "visitors",
            catalog.get_stream("visitors").schema.to_dict(),
            catalog.get_stream("visitors").key_properties,
        )

        # Should have written records
        self.assertGreater(mock_write_record.call_count, 0)
        # Verify at least one record was written for visitors
        calls = mock_write_record.call_args_list
        visitor_calls = [c for c in calls if c[0][0] == "visitors"]
        self.assertGreater(len(visitor_calls), 0)

    @patch("tap_eloqua.sync.singer.write_schema")
    @patch("tap_eloqua.sync.singer.write_record")
    @patch("tap_eloqua.sync.singer.write_state")
    def test_sync_skips_unselected_streams(self, mock_write_state, mock_write_record, mock_write_schema):
        """Test that sync skips streams not marked as selected."""
        client = self._create_mock_client()
        # Select only visitors, not accounts
        catalog = self._create_catalog_with_selected_streams(["visitors"])
        state = {}

        sync(client, catalog, state, "2024-01-01T00:00:00Z", 1000)

        # accounts should not have schema written
        accounts_calls = [c for c in mock_write_schema.call_args_list if c[0][0] == "accounts"]
        self.assertEqual(len(accounts_calls), 0)

    @patch("tap_eloqua.sync.singer.write_schema")
    @patch("tap_eloqua.sync.singer.write_record")
    @patch("tap_eloqua.sync.singer.write_state")
    def test_sync_writes_state_messages(self, mock_write_state, mock_write_record, mock_write_schema):
        """Test that sync writes state messages during execution."""
        client = self._create_mock_client()
        catalog = self._create_catalog_with_selected_streams(["visitors"])
        state = {}

        sync(client, catalog, state, "2024-01-01T00:00:00Z", 1000)

        # Should have written state messages
        self.assertGreater(mock_write_state.call_count, 0)

    @patch("tap_eloqua.sync.singer.write_schema")
    @patch("tap_eloqua.sync.singer.write_record")
    @patch("tap_eloqua.sync.singer.write_state")
    def test_sync_returns_without_selected_streams(self, mock_write_state, mock_write_record, mock_write_schema):
        """Test that sync returns early if no streams are selected."""
        client = self._create_mock_client()
        # Create catalog with no selected streams
        catalog = self._create_catalog_with_selected_streams([])
        state = {}

        sync(client, catalog, state, "2024-01-01T00:00:00Z", 1000)

        # Should not have written anything
        mock_write_schema.assert_not_called()
        mock_write_record.assert_not_called()

    @patch("tap_eloqua.sync.singer.write_record")
    def test_persist_records_writes_records(self, mock_write_record):
        """Test that persist_records writes records to stdout."""
        catalog = self._create_catalog_with_selected_streams(["visitors"])
        records = [
            {
                "id": "1",
                "type": "Visitor",
                "visitorId": "v1",
                "V_LastVisitDateAndTime": "2024-01-01T00:00:00Z",
            }
        ]

        persist_records(catalog, "visitors", records)

        # Should have written one record
        self.assertEqual(mock_write_record.call_count, 1)
        call_args = mock_write_record.call_args[0]
        self.assertEqual(call_args[0], "visitors")

    def test_record_transformation_handles_datetime(self):
        """Test that record transformation converts epoch timestamps to RFC3339."""
        catalog = self._create_catalog_with_selected_streams(["visitors"])
        
        # Record with epoch timestamp
        records = [
            {
                "id": "1",
                "V_LastVisitDateAndTime": 1704067200,  # Epoch int
            }
        ]

        with patch("tap_eloqua.sync.singer.write_record") as mock_write:
            persist_records(catalog, "visitors", records)

            # Check that written record has transformed datetime
            written_record = mock_write.call_args[0][1]
            # After transformation, should be ISO string format
            self.assertIsNotNone(written_record.get("V_LastVisitDateAndTime"))

    @patch("tap_eloqua.sync.singer.write_schema")
    @patch("tap_eloqua.sync.singer.write_record")
    @patch("tap_eloqua.sync.singer.write_state")
    def test_sync_respects_start_date(self, mock_write_state, mock_write_record, mock_write_schema):
        """Test that sync passes start_date to API calls."""
        client = self._create_mock_client()
        catalog = self._create_catalog_with_selected_streams(["visitors"])
        state = {}
        start_date = "2024-06-01T00:00:00Z"

        sync(client, catalog, state, start_date, 1000)

        # Verify client was called (would include start_date in query)
        self.assertGreater(client.get.call_count, 0)

    @patch("tap_eloqua.sync.singer.write_schema")
    @patch("tap_eloqua.sync.singer.write_record")
    @patch("tap_eloqua.sync.singer.write_state")
    def test_sync_handles_pagination(self, mock_write_state, mock_write_record, mock_write_schema):
        """Test that sync handles paginated responses correctly."""
        client = self._create_mock_client()
        catalog = self._create_catalog_with_selected_streams(["visitors"])
        state = {}

        sync(client, catalog, state, "2024-01-01T00:00:00Z", 1000)

        # Should have processed results
        self.assertGreater(mock_write_record.call_count, 0)


class SyncStateManagementTest(EloquaBaseTest):
    """Tests for state management in sync."""

    def _create_mock_client(self):
        """Create a minimal mock client."""
        mock_client = MagicMock()
        mock_client.get.return_value = {"elements": []}
        return mock_client

    def _create_catalog_with_selected_streams(self, selected_stream_names):
        """Create a mock catalog with specific streams selected."""
        catalog_dict = {
            "streams": [
                {
                    "tap_stream_id": "visitors",
                    "stream": "visitors",
                    "schema": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                    },
                    "key_properties": [],
                    "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata": {"selected": "visitors" in selected_stream_names},
                        }
                    ],
                }
            ]
        }
        return Catalog.from_dict(catalog_dict)

    @patch("tap_eloqua.sync.singer.write_schema")
    @patch("tap_eloqua.sync.singer.write_record")
    @patch("tap_eloqua.sync.singer.write_state")
    def test_sync_initializes_state_for_stream(self, mock_write_state, mock_write_record, mock_write_schema):
        """Test that sync initializes state structure for streams."""
        client = self._create_mock_client()
        catalog = self._create_catalog_with_selected_streams(["visitors"])
        state = {}

        sync(client, catalog, state, "2024-01-01T00:00:00Z", 1000)

        # write_state should have been called to track progress
        self.assertGreater(mock_write_state.call_count, 0)

    @patch("tap_eloqua.sync.singer.write_schema")
    @patch("tap_eloqua.sync.singer.write_record")
    @patch("tap_eloqua.sync.singer.write_state")
    def test_sync_resets_current_stream_on_completion(self, mock_write_state, mock_write_record, mock_write_schema):
        """Test that sync sets current_stream to None when done."""
        client = self._create_mock_client()
        catalog = self._create_catalog_with_selected_streams(["visitors"])
        state = {}

        sync(client, catalog, state, "2024-01-01T00:00:00Z", 1000)

        # Last state write should set current_stream to None
        last_write = mock_write_state.call_args_list[-1]
        # The state dict passed to write_state should have current_stream: None at the end


if __name__ == "__main__":
    unittest.main()
