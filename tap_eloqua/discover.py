from requests.exceptions import HTTPError
import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_eloqua.schema import get_schemas, get_pk
from tap_eloqua.constants import STATIC_ENDPOINTS

LOGGER = singer.get_logger()

# Derive probe paths from the shared STATIC_ENDPOINTS constant.
# Each path is prefixed with the REST API base to form a full probe URL.
STATIC_STREAM_PROBE_PATHS = {
    ep['stream_id']: f"/api/REST/2.0/{ep['path']}"
    for ep in STATIC_ENDPOINTS
}


def check_stream_access(stream_name, probe_fn, auth_error_types, fallback_accessible=False):
    """
    Probe a stream endpoint and return True if accessible, False on auth error.

    :param stream_name: Used in log messages.
    :param probe_fn: Zero-argument callable that performs the API probe.
    :param auth_error_types: Exception type(s) indicating 401/403 — returns False.
    :param fallback_accessible: If True, non-auth errors (e.g. 400 from minimal
                                probe params) are treated as auth-OK and return True.
                                If False (default), they are re-raised.
    """
    try:
        probe_fn()
        LOGGER.info("Stream '%s' is accessible.", stream_name)
        return True
    except auth_error_types:
        LOGGER.warning(
            "Stream '%s' is not accessible with the provided credentials.",
            stream_name,
        )
        return False
    except Exception:  # pylint: disable=broad-except
        if fallback_accessible:
            LOGGER.info("Stream '%s' endpoint reachable (auth OK).", stream_name)
            return True
        raise


def _is_auth_http_error(exc):
    """Returns True if an HTTPError indicates a 401/403 response."""
    return exc.response is not None and exc.response.status_code in (401, 403)


class _EloquaAuthError(Exception):
    """Sentinel raised when an HTTPError is a 401/403 to fit the standard checker."""


def _check_stream_access(client, stream_name, probe_path):
    """
    Probes a static stream's REST endpoint with count=1 to verify the
    credentials have access. Returns True if accessible, False on 401/403.
    Any other HTTP error is re-raised.
    """
    def _probe():
        try:
            client.get(probe_path, params={'count': 1}, endpoint=stream_name)
        except HTTPError as exc:
            if _is_auth_http_error(exc):
                raise _EloquaAuthError() from exc
            raise

    return check_stream_access(
        stream_name,
        probe_fn=_probe,
        auth_error_types=_EloquaAuthError,
    )


def discover(client):
    # get_schemas() makes API calls for bulk, activity, and custom-object streams,
    # so access for those is implicitly verified here. Only static streams need an
    # explicit probe since their schemas are loaded from local files.
    schemas, field_metadata = get_schemas(client)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        if stream_name in STATIC_STREAM_PROBE_PATHS:
            if not _check_stream_access(client, stream_name, STATIC_STREAM_PROBE_PATHS[stream_name]):
                continue

        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]
        pk = get_pk(stream_name)

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            schema=schema,
            metadata=mdata
        ))

    if not catalog.streams:
        raise Exception(
            "No stream endpoints are accessible with the provided credentials. "
            "Verify that the API credentials have the required permissions."
        )

    return catalog
