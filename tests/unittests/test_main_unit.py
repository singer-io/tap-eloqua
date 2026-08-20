import io
import json
import importlib
import runpy
import tempfile
import unittest
from argparse import Namespace
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog

tap_main = importlib.import_module("tap_eloqua.__init__")


class TestMainUnit(unittest.TestCase):
    def test_check_config_raises_for_missing_keys(self):
        with self.assertRaises(Exception) as error_context:
            tap_main.check_config({"start_date": "2024-01-01T00:00:00Z"}, ["start_date", "client_id"])
        self.assertIn("missing required keys", str(error_context.exception).lower())

    def test_load_json(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config_file:
            json.dump({"k": "v"}, config_file)
            config_path = config_file.name

        loaded = tap_main.load_json(config_path)
        self.assertEqual(loaded, {"k": "v"})

    @patch("tap_eloqua.__init__.check_config")
    @patch("tap_eloqua.__init__.Catalog.load", return_value=MagicMock())
    @patch("tap_eloqua.__init__.load_json")
    @patch("argparse.ArgumentParser.parse_args")
    def test_parse_args_loads_json_and_catalog(self, mock_parse_args, mock_load_json, mock_catalog_load, mock_check_config):
        mock_parse_args.return_value = Namespace(
            config="config.json",
            state="state.json",
            properties="props.json",
            catalog="catalog.json",
            discover=False,
        )
        mock_load_json.side_effect = [
            {"start_date": "2024-01-01T00:00:00Z", "client_id": "id", "client_secret": "secret", "refresh_token": "rt", "redirect_uri": "uri"},
            {"bookmarks": {}},
            {"streams": []},
        ]

        parsed = tap_main.parse_args(tap_main.REQUIRED_CONFIG_KEYS)

        self.assertEqual(parsed.config_path, "config.json")
        self.assertEqual(parsed.state_path, "state.json")
        self.assertEqual(parsed.properties_path, "props.json")
        self.assertEqual(parsed.catalog_path, "catalog.json")
        mock_catalog_load.assert_called_once_with("catalog.json")
        mock_check_config.assert_called_once()

    @patch("tap_eloqua.__init__.check_config")
    @patch("tap_eloqua.__init__.load_json")
    @patch("argparse.ArgumentParser.parse_args")
    def test_parse_args_sets_empty_state_when_missing(self, mock_parse_args, mock_load_json, _mock_check_config):
        mock_parse_args.return_value = Namespace(
            config="config.json",
            state=None,
            properties=None,
            catalog=None,
            discover=True,
        )
        mock_load_json.return_value = {
            "start_date": "2024-01-01T00:00:00Z",
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt",
            "redirect_uri": "uri",
        }

        parsed = tap_main.parse_args(tap_main.REQUIRED_CONFIG_KEYS)
        self.assertEqual(parsed.state, {})

    def test_do_discover_writes_catalog_json(self):
        mock_client = MagicMock()
        catalog = Catalog.from_dict(
            {
                "streams": [
                    {
                        "tap_stream_id": "s1",
                        "stream": "s1",
                        "schema": {"type": "object", "properties": {"id": {"type": "string"}}},
                        "key_properties": ["id"],
                        "metadata": [{"breadcrumb": [], "metadata": {}}],
                    }
                ]
            }
        )

        stdout_buffer = io.StringIO()
        with patch("tap_eloqua.__init__.discover", return_value=catalog), patch("sys.stdout", stdout_buffer):
            tap_main.do_discover(mock_client)

        output = stdout_buffer.getvalue()
        self.assertIn("tap_stream_id", output)

    @patch("tap_eloqua.__init__.sync")
    @patch("tap_eloqua.__init__.do_discover")
    @patch("tap_eloqua.__init__.parse_args")
    @patch("tap_eloqua.__init__.EloquaClient")
    def test_main_runs_discover_when_flag_set(self, mock_client_class, mock_parse_args, mock_do_discover, mock_sync):
        mock_parse_args.return_value = Namespace(
            config_path="config.json",
            config={
                "client_id": "id",
                "client_secret": "secret",
                "refresh_token": "rt",
                "redirect_uri": "uri",
                "start_date": "2024-01-01T00:00:00Z",
            },
            discover=True,
            catalog=None,
            state={},
        )
        mock_client_context = MagicMock()
        mock_client_class.return_value.__enter__.return_value = mock_client_context

        tap_main.main.__wrapped__()

        mock_do_discover.assert_called_once_with(mock_client_context)
        mock_sync.assert_not_called()

    @patch("tap_eloqua.__init__.sync")
    @patch("tap_eloqua.__init__.do_discover")
    @patch("tap_eloqua.__init__.parse_args")
    @patch("tap_eloqua.__init__.EloquaClient")
    def test_main_runs_sync_when_catalog_present(self, mock_client_class, mock_parse_args, mock_do_discover, mock_sync):
        catalog_obj = MagicMock()
        mock_parse_args.return_value = Namespace(
            config_path="config.json",
            config={
                "client_id": "id",
                "client_secret": "secret",
                "refresh_token": "rt",
                "redirect_uri": "uri",
                "start_date": "2024-01-01T00:00:00Z",
                "bulk_page_size": "123",
            },
            discover=False,
            catalog=catalog_obj,
            state={"bookmarks": {}},
        )
        mock_client_context = MagicMock()
        mock_client_class.return_value.__enter__.return_value = mock_client_context

        tap_main.main.__wrapped__()

        mock_do_discover.assert_not_called()
        mock_sync.assert_called_once_with(
            mock_client_context,
            catalog_obj,
            {"bookmarks": {}},
            "2024-01-01T00:00:00Z",
            123,
        )

    def test_module_main_guard_executes(self):
        with patch("sys.argv", ["tap_eloqua", "--help"]):
            with self.assertRaises(SystemExit):
                runpy.run_module("tap_eloqua.__init__", run_name="__main__")


if __name__ == "__main__":
    unittest.main()
