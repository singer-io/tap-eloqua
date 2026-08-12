from requests.exceptions import HTTPError
import singer
from singer import metadata as mdata
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


def _is_auth_http_error(exc):
    """Returns True if an HTTPError indicates a 401/403 response."""
    return exc.response is not None and exc.response.status_code in (401, 403)


def check_stream_access(client, probe_path, stream_name) -> bool:
    """
    Probes a static stream's REST endpoint with count=1 to verify the
    credentials have access. Returns True if accessible, False on 401/403.
    Any other HTTP error is re-raised.
    """
    try:
        client.get(probe_path, params={'count': 1}, endpoint=stream_name)
        return True
    except HTTPError as exc:
        if _is_auth_http_error(exc):
            return False
        raise


def get_standard_metadata(metadata_map, key_properties, replication_method, replication_key=None):
    metadata_map = mdata.write(metadata_map, (), 'inclusion', 'available')
    metadata_map = mdata.write(metadata_map, (), 'table-key-properties', key_properties)
    metadata_map = mdata.write(metadata_map, (), 'forced-replication-method', replication_method)
    metadata_map = mdata.write(metadata_map, (), 'replication-method', replication_method)
    if replication_key:
        metadata_map = mdata.write(metadata_map, (), 'valid-replication-keys', [replication_key])
        metadata_map = mdata.write(metadata_map, ('properties', replication_key), 'inclusion', 'automatic')
    return metadata_map


def discover(client):
    # get_schemas() makes API calls for bulk, activity, and custom-object streams,
    # so access for those is implicitly verified here. Only static streams need an
    # explicit probe since their schemas are loaded from local files.
    schemas, field_metadata = get_schemas(client)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        if stream_name in STATIC_STREAM_PROBE_PATHS:
            if not check_stream_access(client, STATIC_STREAM_PROBE_PATHS[stream_name], stream_name):
                LOGGER.warning(
                    "Stream '%s' will be excluded from the catalog due to insufficient permissions.",
                    stream_name,
                )
                continue

        schema = Schema.from_dict(schema_dict)
        properties = schema_dict.get("properties", {})
        # Accept both "updatedAt" (camelCase) and "updatedat" (lowercase) as replication keys.
        # Eloqua schemas may use either casing depending on whether the stream is a
        # bulk/activity stream ("updatedAt") or a static schema ("UpdatedAt").
        replication_key = next(
            (k for k in properties if k.lower() == "updatedat" or k == "updatedAt"),
            None
        )
        replication_method = "INCREMENTAL" if replication_key else "FULL_TABLE"
        md_list = field_metadata[stream_name]
        m = mdata.to_map(md_list)
        pk = get_pk(stream_name)
        m = get_standard_metadata(m, pk, replication_method, replication_key)
        md_list = mdata.to_list(m)

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            schema=schema,
            metadata=md_list,
            replication_key=replication_key
        ))

    if not catalog.streams:
        raise Exception(
            "The credentials do not have read access to any of the supported streams."
        )

    return catalog
