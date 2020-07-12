import hashlib
import json
from datetime import timedelta
import re

import singer
from singer.utils import strftime, strptime_to_utc
from tap_amazon_ads_dsp.schema import dimension_primary_keys, fields_for_report_dimensions, report_dimension_metrics
from decimal import *

LOGGER = singer.get_logger()


# Convert camelCase to snake_case
def convert(name):
    regsub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', regsub).lower()


# Convert keys in json array
def convert_array(arr):
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(this_json):
    out = {}
    if isinstance(this_json, dict):
        for key in this_json:
            new_key = convert(key)
            if isinstance(this_json[key], dict):
                out[new_key] = convert_json(this_json[key])
            elif isinstance(this_json[key], list):
                out[new_key] = convert_array(this_json[key])
            else:
                out[new_key] = this_json[key]
    else:
        return convert_array(this_json)
    return out

# Create MD5 hash key for data element
def hash_data(data):
    # Prepare the project id hash
    hash_id = hashlib.md5()
    hash_id.update(repr(data).encode('utf-8'))
    return hash_id.hexdigest()

def transform_record(report_name, report_type, report_date, report_dimensions, record):
    report_primary_keys = fields_for_report_dimensions(report_type, report_dimensions)
    transformed_report = convert_json(record)
    primary_keys = primary_keys_for_record(report_primary_keys, record)
    dims_md5 = str(hash_data(json.dumps(primary_keys, sort_keys=True)))
    record['__sdc_record_hash'] = dims_md5
    record['report_date'] = report_date.strftime('%Y-%m-%dT%H:%M:%S%z')
    return record

# Transform for report_data in sync_report
def transform_report(schema, report_name, report_type, report_date, report_dimensions, report_data):
    report_primary_keys = fields_for_report_dimensions(report_type, report_dimensions)
    # camel_keys = []
    # for key in report_primary_keys:
    #     camel_keys.append(convert(key))
    # transformed_records = []

    # Camel to Snake case
    # transformed_report = convert_json(report_data)

    for record in report_data:
        primary_keys = primary_keys_for_record(report_primary_keys, record)
        dims_md5 = str(hash_data(json.dumps(primary_keys, sort_keys=True)))
        record['__sdc_record_hash'] = dims_md5
        record['report_date'] = report_date.strftime('%Y-%m-%dT%H:%M:%S%z')

        # Convert any percentages to floats
        for field, value in record.items():
            if '%' in value:
                ## Set precision of percentage fields based on schema multipleOf
                precision = schema['properties'][field].get('multipleOf', 0.000001)
                dec_ex = abs(Decimal(f'{precision}').as_tuple().exponent)
                new_value = value.strip('%')
                new_dec = round(Decimal(new_value)/100, dec_ex) 
                record[field] = new_dec

        # transformed_records.append(record)
    LOGGER.info(f"Transformed batch of {len(report_data)}")
    return transformed_report

def primary_keys_for_record(report_primary_keys, record):
    primary_keys_for_record = {}
    for key in report_primary_keys:
        primary_keys_for_record[key] = record.get(key)
    return primary_keys_for_record


def date_to_epoch(report_date):
    epoch_time = strptime_to_utc("19700101")
    report_datetime = strptime_to_utc(str(report_date))
    return int((report_datetime - epoch_time).total_seconds())
