import hashlib
import json
from datetime import timedelta

import singer
from singer.utils import strftime, strptime_to_utc
from tap_amazon_advertising_dsp.schema import (DIMENSION_PRIMARY_KEYS,
                                               PRIMARY_KEYS)

LOGGER = singer.get_logger()


# Create MD5 hash key for data element
def hash_data(data):
    # Prepare the project id hash
    hash_id = hashlib.md5()
    hash_id.update(repr(data).encode('utf-8'))
    return hash_id.hexdigest()


# Transform for report_data in sync_report
def transform_report(report_name, report_dimensions, report_data):
    # Loop through entity_id records w/ data
    report_primary_keys = []
    for key in PRIMARY_KEYS:
        report_primary_keys.append(key)
    for dimension in report_dimensions:
        report_primary_keys.append(DIMENSION_PRIMARY_KEYS.get(dimension))

    transformed_records = []
    for record in report_data:
        # Create MD5 hash key of sorted json dimesions (above)
        # get values for all primary keys
        primary_keys = primary_keys_for_record(report_primary_keys, record)
        dims_md5 = str(hash_data(json.dumps(primary_keys, sort_keys=True)))
        record['__sdc_dimensions_hash_key'] = dims_md5
        transformed_records.append(record)
    return transformed_records


def primary_keys_for_record(report_primary_keys, record):
    try:
        primary_keys_for_record = [record[x] for x in report_primary_keys]
    except KeyError as ex:
        raise Exception((f'Primary key {ex.message} not found in {record}'))
    return primary_keys_for_record
