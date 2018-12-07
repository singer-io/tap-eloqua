from singer.catalog import Catalog, CatalogEntry, Schema

from tap_eloqua.schema import get_schemas, get_pk

def discover(client):
    schemas, field_metadata = get_schemas(client)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        metadata = field_metadata[stream_name]
        pk = get_pk(stream_name)

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            schema=schema,
            metadata=metadata
        ))

    return catalog
