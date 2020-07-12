import hashlib
import json
from decimal import Decimal

import singer
from tap_amazon_ads_dsp.schema import fields_for_report_dimensions

LOGGER = singer.get_logger()


# Create MD5 hash key for data element
def hash_data(data):
    # Prepare the project id hash
    hash_id = hashlib.md5()
    hash_id.update(repr(data).encode('utf-8'))
    return hash_id.hexdigest()

# Transform for report_data in sync_report
# - Add __sdc_record_hash
# - Add report_date as API inconcistent across report type
# - Convert percentage strings to decimals of schema defined precision
def transform_report(schema, report_type, report_date,
                     report_dimensions, report_data):
    report_primary_keys = fields_for_report_dimensions(report_type,
                                                       report_dimensions)

    for record in report_data:
        primary_keys = primary_keys_for_record(report_primary_keys, record)
        dims_md5 = str(hash_data(json.dumps(primary_keys, sort_keys=True)))
        record['__sdc_record_hash'] = dims_md5
        record['report_date'] = report_date.strftime('%Y-%m-%dT%H:%M:%S%z')

        # Convert any percentages to floats
        for field, value in record.items():
            if isinstance(value, str) and '%' in value:
                ## Set precision of percentage fields based on schema multipleOf
                precision = schema['properties'][field].get(
                    'multipleOf', 0.000001)
                dec_ex = abs(Decimal(f'{precision}').as_tuple().exponent)
                new_value = value.strip('%')
                new_dec = round(Decimal(new_value) / 100, dec_ex)
                record[field] = new_dec


def primary_keys_for_record(report_primary_keys, record):
    keys = {}
    for key in report_primary_keys:
        keys[key] = record.get(key)
    return keys
