from requests.exceptions import HTTPError
import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata as mdata  # minimal addition

from tap_eloqua.schema import get_schemas, get_pk
from tap_eloqua.constants import STATIC_ENDPOINTS

LOGGER = singer.get_logger()

# Derive probe paths from the shared STATIC_ENDPOINTS constant.
# Each path is prefixed with the REST API base to form a full probe URL.
STATIC_STREAM_PROBE_PATHS = {
    ep['stream_id']: f"/api/REST/2.0/{ep['path']}"
    for ep in STATIC_ENDPOINTS
}

#They are treated as children of contacts by data model linkage,
# not by URL nesting.
PARENT_STREAM_MAP = {
    "activity_email_open": "contacts",
    "activity_email_clickthrough": "contacts",
    "activity_email_send": "contacts",
    "activity_subscribe": "contacts",
    "activity_unsubscribe": "contacts",
    "activity_bounceback": "contacts",
    "activity_web_visit": "contacts",
    "activity_page_view": "contacts",
    "activity_form_submit": "contacts",
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
            LOGGER.warning(
                    "Permission Error: Stream '%s' - %s",
                    stream_name,
                    exc,
                )
            return False
        raise


def _get_child_streams(parent_stream, schemas):
    """Return mapped child streams that exist in discovered schemas."""
    return [
        child_stream
        for child_stream, mapped_parent in PARENT_STREAM_MAP.items()
        if mapped_parent == parent_stream and child_stream in schemas
    ]


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Probe static streams for read access and exclude inaccessible streams in place."""
    inaccessible_streams = [
        stream_name
        for stream_name in list(schemas.keys())
        if stream_name in STATIC_STREAM_PROBE_PATHS
        and not check_stream_access(client, STATIC_STREAM_PROBE_PATHS[stream_name], stream_name)
    ]

    child_streams_to_exclude = []
    for stream_name in inaccessible_streams:
        child_streams_to_exclude.extend(_get_child_streams(stream_name, schemas))

    inaccessible_streams.extend(child_streams_to_exclude)
    inaccessible_streams = list(dict.fromkeys(inaccessible_streams))

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    if not schemas:
        raise Exception(
            "No stream endpoints are accessible with the provided credentials."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(inaccessible_streams),
        )


def discover(client):
    # get_schemas() makes API calls for bulk, activity, and custom-object streams,
    # so access for those is implicitly verified here. Only static streams need an
    # explicit probe since their schemas are loaded from local files.
    schemas, field_metadata = get_schemas(client)
    _apply_access_checks(client, schemas, field_metadata)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        pk = get_pk(stream_name)
        properties = schema_dict.get("properties", {})
        replication_key = next(
            (k for k in properties if k.lower() == "updatedat"),
            None
        )
        replication_method = "INCREMENTAL" if replication_key else "FULL_TABLE"
        valid_replication_keys = [replication_key] if replication_key else []
        md_list = field_metadata[stream_name]
        m = mdata.to_map(md_list)
        m = mdata.write(m, (), 'table-key-properties', pk)
        m = mdata.write(m, (), 'forced-replication-method', replication_method)
        m = mdata.write(m, (), 'valid-replication-keys', valid_replication_keys)
        if stream_name in PARENT_STREAM_MAP:
            m = mdata.write(m, (), 'parent-stream-id', PARENT_STREAM_MAP[stream_name])
        md_list = mdata.to_list(m)
        key_properties = mdata.to_map(md_list).get((), {}).get('table-key-properties', [])
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=key_properties,
            schema=schema,
            metadata=md_list
        ))

    return catalog
