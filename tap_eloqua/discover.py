import singer
from requests.exceptions import HTTPError
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_eloqua.schema import get_schemas, get_pk
from tap_eloqua.utils import check_stream_access

LOGGER = singer.get_logger()

# Static streams load schemas from local JSON files — no API call is made during
# get_schemas(). Map each to a lightweight probe path to verify access at discovery time.
STATIC_STREAM_PROBE_PATHS = {
    'visitors':    '/api/REST/2.0/data/visitors',
    'campaigns':   '/api/REST/2.0/assets/campaigns',
    'emails':      '/api/REST/2.0/assets/emails',
    'forms':       '/api/REST/2.0/assets/forms',
    'assets':      '/api/REST/2.0/assets/externals',
    'emailGroups': '/api/REST/2.0/assets/email/groups',
}


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

    return catalog
