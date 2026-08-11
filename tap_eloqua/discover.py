from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata as mdata  # minimal addition

from tap_eloqua.schema import get_schemas, get_pk


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
    schemas, field_metadata = get_schemas(client)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        properties = schema_dict.get("properties", {})
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

    return catalog
