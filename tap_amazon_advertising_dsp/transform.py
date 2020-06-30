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


# Transform for record in async_report
def transform_record(report_primary_keys, report_name, report_type, report_date, report_dimensions, record):
    # Loop through entity_id records w/ data
    report_primary_keys = []
    for key in PRIMARY_KEYS:
        report_primary_keys.append(key)
    for dimension in report_dimensions:
        report_primary_keys.append(DIMENSION_PRIMARY_KEYS.get(dimension))
    report_primary_keys.sort()

    if report_type == 'AUDIENCE':
            epoch_time = strptime_to_utc("19700101")
            report_datetime = strptime_to_utc(str(report_date))
            report_date_epoch = int((report_datetime - epoch_time).total_seconds())
            record['date'] = report_date_epoch
    # Create MD5 hash key of sorted json dimesions (above)
    # get values for all primary keys
    primary_keys = primary_keys_for_record(report_primary_keys, record)
    dims_md5 = str(hash_data(json.dumps(primary_keys, sort_keys=True)))
    record['__sdc_dimensions_hash_key'] = dims_md5
    # return record.append(record)
    # return transformed_records
    return record


# Transform for report_data in sync_report
def transform_report(report_name, report_type, report_date, report_dimensions, report_data):
    # Loop through entity_id records w/ data
    report_primary_keys = []
    for key in PRIMARY_KEYS:
        report_primary_keys.append(key)
    for dimension in report_dimensions:
        report_primary_keys.append(DIMENSION_PRIMARY_KEYS.get(dimension))
    report_primary_keys.sort()

    transformed_records = []

    if report_type == 'AUDIENCE':
        report_date_epoch = date_to_epoch(report_date)

    for record in report_data:
        # Add report date to Audience data which API does not include
        if report_type == 'AUDIENCE':
            record['date'] = report_date_epoch
        primary_keys = primary_keys_for_record(report_primary_keys, record)
        dims_md5 = str(hash_data(json.dumps(primary_keys, sort_keys=True)))
        record['__sdc_record_hash'] = dims_md5
        transformed_records.append(record)
    return transformed_records


# Todo: Add key and value to hash - DONE
# Todo: Sort by key - DONE
# Todo: Add report date - DONE
def primary_keys_for_record(report_primary_keys, record):
    primary_keys_for_record = {}
    for key in report_primary_keys:
        primary_keys_for_record[key] = record.get(key)
    return primary_keys_for_record


def date_to_epoch(report_date):
    epoch_time = strptime_to_utc("19700101")
    report_datetime = strptime_to_utc(str(report_date))
    return int((report_datetime - epoch_time).total_seconds())