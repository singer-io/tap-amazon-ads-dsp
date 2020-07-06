import hashlib
import json
from datetime import timedelta

import singer
from singer.utils import strftime, strptime_to_utc
from tap_amazon_ads_dsp.schema import dimension_primary_keys, fields_for_report_dimensions, report_dimension_metrics

LOGGER = singer.get_logger()


# Create MD5 hash key for data element
def hash_data(data):
    # Prepare the project id hash
    hash_id = hashlib.md5()
    hash_id.update(repr(data).encode('utf-8'))
    return hash_id.hexdigest()

# Transform for report_data in sync_report
def transform_report(report_name, report_type, report_date, report_dimensions, report_data):
    report_primary_keys = fields_for_report_dimensions(report_type, report_dimensions)
    transformed_records = []

    for record in report_data:
        primary_keys = primary_keys_for_record(report_primary_keys, record)
        dims_md5 = str(hash_data(json.dumps(primary_keys, sort_keys=True)))
        record['__sdc_record_hash'] = dims_md5
        record['report_date'] = report_date.strftime('%Y%m%d')
        transformed_records.append(record)
    return transformed_records

def primary_keys_for_record(report_primary_keys, record):
    primary_keys_for_record = {}
    for key in report_primary_keys:
        primary_keys_for_record[key] = record.get(key)
    return primary_keys_for_record


def date_to_epoch(report_date):
    epoch_time = strptime_to_utc("19700101")
    report_datetime = strptime_to_utc(str(report_date))
    return int((report_datetime - epoch_time).total_seconds())