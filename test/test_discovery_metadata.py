import unittest
from singer import metadata as mdata
from tap_eloqua import discover
from unittest.mock import MagicMock

class TestEloquaDiscoveryMetadata(unittest.TestCase):
    def test_replication_methods_for_known_streams(self):
        client = MagicMock()
        catalog = discover(client)

        # Hardcoded expected replication methods for certain streams:
        expected_methods = {
            "accounts": "INCREMENTAL",
            "contacts": "INCREMENTAL",
            "visitors": "FULL_TABLE",
        }
        for s in catalog.streams:
            stream_name = s.stream.lower()
            if stream_name not in expected_methods:
                continue

            md_map = mdata.to_map(s.metadata)
            self.assertIn((), md_map, msg=f"Root metadata missing for stream {stream_name}")
            root = md_map[()]
            self.assertEqual(
                root.get("forced-replication-method"),
                expected_methods[stream_name],
                msg=f"Replication method mismatch for stream {stream_name}"
            )

if __name__ == "__main__":
    unittest.main()