"""Unit tests for tap_eloqua discover module and check_stream_access helper."""
import unittest
from unittest.mock import MagicMock, patch
from requests.exceptions import HTTPError
from requests import Response

from tap_eloqua.discover import (
    check_stream_access,
    _is_auth_http_error,
    _apply_access_checks,
    STATIC_STREAM_PROBE_PATHS,
    discover,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_http_error(status_code):
    """Build a requests.HTTPError with a stubbed response."""
    resp = MagicMock(spec=Response)
    resp.status_code = status_code
    err = HTTPError(response=resp)
    return err


# ---------------------------------------------------------------------------
# check_stream_access
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):
    """Tests for the merged check_stream_access function in tap_eloqua.discover."""

    def test_returns_true_when_client_succeeds(self):
        client = MagicMock()
        result = check_stream_access(client, "/api/REST/2.0/assets/campaigns", "campaigns")
        self.assertTrue(result)
        client.get.assert_called_once()

    def test_returns_false_on_401(self):
        client = MagicMock()
        client.get.side_effect = _make_http_error(401)
        result = check_stream_access(client, "/api/REST/2.0/assets/campaigns", "campaigns")
        self.assertFalse(result)

    def test_returns_false_on_403(self):
        client = MagicMock()
        client.get.side_effect = _make_http_error(403)
        result = check_stream_access(client, "/api/REST/2.0/data/visitors", "visitors")
        self.assertFalse(result)

    def test_re_raises_non_auth_http_error(self):
        """HTTPError with status other than 401/403 should propagate."""
        client = MagicMock()
        client.get.side_effect = _make_http_error(500)
        with self.assertRaises(HTTPError):
            check_stream_access(client, "/api/REST/2.0/assets/campaigns", "campaigns")

    def test_probe_passes_count_param(self):
        """Verifies the probe uses count=1 for minimal data."""
        client = MagicMock()
        check_stream_access(client, "/api/REST/2.0/assets/emails", "emails")
        call_kwargs = client.get.call_args.kwargs
        self.assertIn("params", call_kwargs)
        self.assertEqual(call_kwargs["params"].get("count"), 1)

class TestIsAuthHttpError(unittest.TestCase):

    def test_returns_true_for_401(self):
        self.assertTrue(_is_auth_http_error(_make_http_error(401)))

    def test_returns_true_for_403(self):
        self.assertTrue(_is_auth_http_error(_make_http_error(403)))

    def test_returns_false_for_404(self):
        self.assertFalse(_is_auth_http_error(_make_http_error(404)))

    def test_returns_false_for_500(self):
        self.assertFalse(_is_auth_http_error(_make_http_error(500)))

    def test_returns_false_when_response_is_none(self):
        err = HTTPError()
        err.response = None
        self.assertFalse(_is_auth_http_error(err))

# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):
    """Tests for the discover() function in tap_eloqua.discover."""

    _STATIC_STREAMS = list(STATIC_STREAM_PROBE_PATHS.keys())
    _DYNAMIC_STREAMS = ["contacts", "accounts", "activity_email_open"]

    def _all_stream_names(self):
        return self._STATIC_STREAMS + self._DYNAMIC_STREAMS

    def _mock_schemas(self, stream_names):
        schemas = {n: {"type": "object", "properties": {}} for n in stream_names}
        meta = {n: [] for n in stream_names}
        return schemas, meta

    @patch("tap_eloqua.discover.check_stream_access")
    def test_apply_access_checks_excludes_inaccessible_static_streams(self, mock_check):
        names = self._all_stream_names()
        schemas, field_metadata = self._mock_schemas(names)
        blocked = "campaigns"
        mock_check.side_effect = lambda client, path, name: name != blocked

        _apply_access_checks(MagicMock(), schemas, field_metadata)

        self.assertNotIn(blocked, schemas)
        self.assertNotIn(blocked, field_metadata)
        for dyn in self._DYNAMIC_STREAMS:
            self.assertIn(dyn, schemas)

    @patch("tap_eloqua.discover.PARENT_STREAM_MAP", {"activity_email_open": "campaigns"})
    @patch("tap_eloqua.discover.check_stream_access")
    def test_apply_access_checks_excludes_child_streams_of_inaccessible_parent(self, mock_check):
        names = self._all_stream_names()
        schemas, field_metadata = self._mock_schemas(names)
        blocked_parent = "campaigns"
        blocked_child = "activity_email_open"
        mock_check.side_effect = lambda client, path, name: name != blocked_parent

        _apply_access_checks(MagicMock(), schemas, field_metadata)

        self.assertNotIn(blocked_parent, schemas)
        self.assertNotIn(blocked_child, schemas)
        self.assertNotIn(blocked_parent, field_metadata)
        self.assertNotIn(blocked_child, field_metadata)

    @patch("tap_eloqua.discover.check_stream_access", return_value=False)
    def test_apply_access_checks_raises_when_no_streams_remain(self, _mock_check):
        schemas, field_metadata = self._mock_schemas(self._STATIC_STREAMS)

        with self.assertRaises(Exception) as ctx:
            _apply_access_checks(MagicMock(), schemas, field_metadata)

        self.assertIn("No stream endpoints are accessible", str(ctx.exception))

    @patch("tap_eloqua.discover.check_stream_access")
    @patch("tap_eloqua.discover.get_schemas")
    def test_static_streams_probed_dynamic_streams_not(self, mock_get_schemas, mock_check):
        """check_stream_access is called only for static streams (not dynamic)."""
        all_names = self._all_stream_names()
        mock_get_schemas.return_value = self._mock_schemas(all_names)
        mock_check.return_value = True

        client = MagicMock()
        discover(client)

        checked_streams = {call.args[2] for call in mock_check.call_args_list}
        self.assertEqual(checked_streams, set(self._STATIC_STREAMS))
        for dyn in self._DYNAMIC_STREAMS:
            self.assertNotIn(dyn, checked_streams)

    @patch("tap_eloqua.discover.check_stream_access")
    @patch("tap_eloqua.discover.get_schemas")
    def test_inaccessible_static_stream_excluded(self, mock_get_schemas, mock_check):
        """A static stream returning False is excluded from the catalog."""
        all_names = self._all_stream_names()
        mock_get_schemas.return_value = self._mock_schemas(all_names)
        blocked = "campaigns"
        mock_check.side_effect = lambda client, path, name: name != blocked

        client = MagicMock()
        catalog = discover(client)
        returned = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn(blocked, returned)

    @patch("tap_eloqua.discover.check_stream_access")
    @patch("tap_eloqua.discover.get_schemas")
    def test_dynamic_streams_always_included(self, mock_get_schemas, mock_check):
        """Dynamic streams bypass the access check and are always in the catalog."""
        all_names = self._all_stream_names()
        mock_get_schemas.return_value = self._mock_schemas(all_names)
        mock_check.return_value = False  # all static streams blocked

        client = MagicMock()
        catalog = discover(client)
        returned = {s.tap_stream_id for s in catalog.streams}
        for dyn in self._DYNAMIC_STREAMS:
            self.assertIn(dyn, returned)

    @patch("tap_eloqua.discover.check_stream_access")
    @patch("tap_eloqua.discover.get_schemas")
    def test_all_static_inaccessible_keeps_dynamic(self, mock_get_schemas, mock_check):
        """When all static streams are blocked, only dynamic streams remain."""
        all_names = self._all_stream_names()
        mock_get_schemas.return_value = self._mock_schemas(all_names)
        mock_check.return_value = False

        client = MagicMock()
        catalog = discover(client)
        returned = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(returned, set(self._DYNAMIC_STREAMS))

    @patch("tap_eloqua.discover.check_stream_access")
    @patch("tap_eloqua.discover.get_schemas")
    def test_raises_when_catalog_is_empty(self, mock_get_schemas, mock_check):
        """discover() raises Exception when no streams are accessible (empty catalog)."""
        # Only static streams returned — all blocked, so catalog would be empty.
        mock_get_schemas.return_value = self._mock_schemas(self._STATIC_STREAMS)
        mock_check.return_value = False

        client = MagicMock()
        with self.assertRaises(Exception) as ctx:
            discover(client)
        self.assertIn("No stream endpoints are accessible", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
