import json
import os
import tempfile
import unittest
from unittest import mock

from singer.catalog import Catalog

import tap_eloqua


class TestTapEloquaMainUnit(unittest.TestCase):
    def test_check_config_raises_on_missing_keys(self):
        with self.assertRaises(Exception) as exc:
            tap_eloqua.check_config({"start_date": "2020-01-01T00:00:00Z"}, ["start_date", "client_id"])
        self.assertIn("missing required keys", str(exc.exception).lower())

    def test_check_config_passes_when_all_keys_exist(self):
        tap_eloqua.check_config(
            {
                "start_date": "2020-01-01T00:00:00Z",
                "client_id": "id",
            },
            ["start_date", "client_id"],
        )

    def test_load_json(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            json.dump({"k": "v"}, tmp)
            path = tmp.name
        try:
            data = tap_eloqua.load_json(path)
            self.assertEqual(data, {"k": "v"})
        finally:
            os.remove(path)

    def test_parse_args_loads_config_state_properties_and_catalog(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.json")
            state_path = os.path.join(tmpdir, "state.json")
            props_path = os.path.join(tmpdir, "properties.json")
            catalog_path = os.path.join(tmpdir, "catalog.json")

            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "start_date": "2020-01-01T00:00:00Z",
                        "client_id": "id",
                        "client_secret": "secret",
                        "refresh_token": "refresh",
                        "redirect_uri": "http://localhost/callback",
                    },
                    f,
                )
            with open(state_path, "w", encoding="utf-8") as f:
                json.dump({"bookmarks": {}}, f)
            with open(props_path, "w", encoding="utf-8") as f:
                json.dump({"streams": []}, f)

            catalog_dict = {
                "streams": [
                    {
                        "stream": "accounts",
                        "tap_stream_id": "accounts",
                        "schema": {"type": "object", "properties": {"Id": {"type": "string"}}},
                        "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
                        "key_properties": ["Id"],
                    }
                ]
            }
            with open(catalog_path, "w", encoding="utf-8") as f:
                json.dump(catalog_dict, f)

            argv = [
                "prog",
                "--config",
                config_path,
                "--state",
                state_path,
                "--properties",
                props_path,
                "--catalog",
                catalog_path,
            ]

            with mock.patch("sys.argv", argv):
                parsed = tap_eloqua.parse_args(tap_eloqua.REQUIRED_CONFIG_KEYS)

            self.assertEqual(parsed.config_path, config_path)
            self.assertEqual(parsed.state_path, state_path)
            self.assertEqual(parsed.properties_path, props_path)
            self.assertEqual(parsed.catalog_path, catalog_path)
            self.assertEqual(parsed.state, {"bookmarks": {}})
            self.assertIsInstance(parsed.catalog, Catalog)

    def test_parse_args_defaults_state_when_missing(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as cfg:
            json.dump(
                {
                    "start_date": "2020-01-01T00:00:00Z",
                    "client_id": "id",
                    "client_secret": "secret",
                    "refresh_token": "refresh",
                    "redirect_uri": "http://localhost/callback",
                },
                cfg,
            )
            cfg_path = cfg.name

        try:
            with mock.patch("sys.argv", ["prog", "--config", cfg_path]):
                parsed = tap_eloqua.parse_args(tap_eloqua.REQUIRED_CONFIG_KEYS)
            self.assertEqual(parsed.state, {})
        finally:
            os.remove(cfg_path)

    @mock.patch("tap_eloqua.json.dump")
    @mock.patch("tap_eloqua.discover")
    def test_do_discover(self, mock_discover, mock_json_dump):
        fake_catalog = mock.MagicMock()
        fake_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = fake_catalog

        tap_eloqua.do_discover(mock.MagicMock())

        mock_discover.assert_called_once()
        mock_json_dump.assert_called_once()

    @mock.patch("tap_eloqua.sync")
    @mock.patch("tap_eloqua.do_discover")
    @mock.patch("tap_eloqua.EloquaClient")
    @mock.patch("tap_eloqua.parse_args")
    def test_main_discover_path(
        self, mock_parse_args, mock_client_cls, mock_do_discover, mock_sync
    ):
        parsed = mock.MagicMock()
        parsed.config_path = "config.json"
        parsed.config = {
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "refresh",
            "redirect_uri": "uri",
            "start_date": "2020-01-01T00:00:00Z",
            "user_agent": "ua",
        }
        parsed.discover = True
        parsed.catalog = None
        parsed.state = {}
        mock_parse_args.return_value = parsed

        client_ctx = mock.MagicMock()
        mock_client_cls.return_value.__enter__.return_value = client_ctx

        tap_eloqua.main.__wrapped__()

        mock_do_discover.assert_called_once_with(client_ctx)
        mock_sync.assert_not_called()

    @mock.patch("tap_eloqua.sync")
    @mock.patch("tap_eloqua.do_discover")
    @mock.patch("tap_eloqua.EloquaClient")
    @mock.patch("tap_eloqua.parse_args")
    def test_main_sync_path(
        self, mock_parse_args, mock_client_cls, mock_do_discover, mock_sync
    ):
        parsed = mock.MagicMock()
        parsed.config_path = "config.json"
        parsed.config = {
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "refresh",
            "redirect_uri": "uri",
            "start_date": "2020-01-01T00:00:00Z",
            "bulk_page_size": "1234",
            "user_agent": "ua",
        }
        parsed.discover = False
        parsed.catalog = {"streams": []}
        parsed.state = {"bookmarks": {}}
        mock_parse_args.return_value = parsed

        client_ctx = mock.MagicMock()
        mock_client_cls.return_value.__enter__.return_value = client_ctx

        tap_eloqua.main.__wrapped__()

        mock_do_discover.assert_not_called()
        mock_sync.assert_called_once_with(
            client_ctx,
            parsed.catalog,
            parsed.state,
            "2020-01-01T00:00:00Z",
            1234,
        )
