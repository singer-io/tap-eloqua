import unittest
from singer import metadata as mdata
from tap_eloqua import discover
from unittest.mock import MagicMock

class TestEloquaDiscoveryMetadata(unittest.TestCase):

    def test_metadata_updates(self):
        client = MagicMock()
        catalog = discover(client)

        for s in catalog.streams:
            md_map = mdata.to_map(s.metadata)
            self.assertIn((), md_map, msg=f"Root metadata missing for stream {s.stream}")
            root = md_map[()]

            # compute expected replication method from schema properties
            schema_props = s.schema.to_dict().get("properties", {})
            expected_rep_method = "INCREMENTAL" if "UpdatedAt" in schema_props else "FULL_TABLE"

            self.assertEqual(root.get("forced-replication-method"),
                             expected_rep_method,
                             msg=f"forced-replication-method mismatch for stream {s.stream}")

if __name__ == "__main__":
    unittest.main()
