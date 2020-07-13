from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_amazon_ads_dsp.schema import get_schemas


def discover(reports):
    schemas, field_metadata = get_schemas(reports)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        # table_metadata = {}
        for entry, value in mdata.items():
            if entry == ():
                table_metadata = value
        key_properties = table_metadata.get('table-key-properties')

        catalog.streams.append(
            CatalogEntry(stream=stream_name,
                         tap_stream_id=stream_name,
                         key_properties=key_properties,
                         schema=schema,
                         metadata=metadata.to_list(mdata)))

    return catalog
