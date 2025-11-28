from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata as mdata  # minimal addition

from tap_eloqua.schema import get_schemas, get_pk

def discover(client):
    schemas, field_metadata = get_schemas(client)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)

        # existing field-level metadata from schema.py
        md_list = field_metadata[stream_name]
        m = mdata.to_map(md_list)
        m = mdata.write(m, (), 'forced-replication-method', 'FULL_TABLE')
        md_list = mdata.to_list(m)

        pk = get_pk(stream_name)

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            schema=schema,
            metadata=md_list
        ))

    return catalog
