# pylint: disable=too-many-lines
from datetime import datetime, timedelta
import time
from urllib.parse import urlparse
import pytz
import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc

from tap_amazon_advertising_dsp.transform import transform_record, transform_report
from tap_amazon_advertising_dsp.schema import REPORT_TYPE_DIMENSION_METRICS
import json
# from tap_amazon_advertising_dsp.streams import flatten_streams

LOGGER = singer.get_logger()
REPORT_GRANULARITY = 'day'

def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    LOGGER.info('Stream: {} - Writing schema'.format(stream_name))
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.error('Stream: {} - OS Error writing schema'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(
            stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error('Stream: {} - OS Error writing record'.format(stream_name))
        LOGGER.error('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, default):
    # default only populated on initial sync
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Stream: {} - Write state, bookmark value: {}'.format(stream, value))
    singer.write_state(state)


# Converts cursor object to dictionary
def obj_to_dict(obj):
    if not hasattr(obj, "__dict__"):
        return obj
    result = {}
    for key, val in obj.__dict__.items():
        if key.startswith("_"):
            continue
        element = []
        if isinstance(val, list):
            for item in val:
                element.append(obj_to_dict(item))
        else:
            element = obj_to_dict(val)
        result[key] = element
    return result


def get_resource(stream_name, client, path, params=None):
    try:
        request = Request(client, 'get', resource, params=params) #, stream=True)
    except Exception as err:
        LOGGER.error('Stream: {} - ERROR: {}'.format(stream_name, err))
        raise err
    cursor = Cursor(None, request)
    return cursor


def post_resource(report_name, entity, client, body=None):
    try:
        response = client._make_request('post', entity, body=body)
    except Exception as err:
        LOGGER.error('Report: {} - ERROR: {}'.format(report_name, err))
        raise err
    response_body = response.json()
    return response_body


def get_async_data(report_name, client, queued_report):
    url = queued_report.get('job_result').get('location')
    try:
        response = Request(
            client, 'get', url, raw_body=True, stream=True).perform()
        response_body = response.body
    except Exception as err:
        LOGGER.error('Report: {} - ERROR: {}'.format(report_name, err.details))
        raise err
    return response_body


# List selected fields from stream catalog
def get_selected_fields(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    mdata = metadata.to_map(stream.metadata)
    mdata_list = singer.metadata.to_list(mdata)
    selected_fields = []
    for entry in mdata_list:
        field = None
        try:
            field = entry['breadcrumb'][1]
            if entry.get('metadata', {}).get('selected', False):
                selected_fields.append(field)
        except IndexError:
            pass
    return selected_fields


def remove_minutes_local(dttm, tzone):
    new_dttm = dttm.astimezone(tzone).replace(
        minute=0, second=0, microsecond=0)
    return new_dttm


def remove_hours_local(dttm):
    new_dttm = dttm.replace(
        hour=0, minute=0, second=0, microsecond=0)
    return new_dttm


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)
    LOGGER.info('Stream: {} - Currently Syncing'.format(stream_name))

# Round start and end times based on granularity and timezone
def round_times(start=None, end=None):
    start_rounded = None
    end_rounded = None
    # Round min_start, max_end to hours or dates
    start_rounded = remove_hours_local(start) - timedelta(days=1)
    end_rounded = remove_hours_local(end) + timedelta(days=1)
    return start_rounded, end_rounded


# Determine absolute start and end times w/ attribution_window constraint
# abs_start/end and window_start/end must be rounded to nearest hour or day (granularity)
def get_absolute_start_end_time(REPORT_GRANULARITY, last_dttm, attribution_window):
    now_dttm = utils.now()
    delta_days = (now_dttm - last_dttm).days
    if delta_days < attribution_window:
        start = now_dttm - timedelta(days=attribution_window)
    else:
        start = last_dttm
    abs_start, abs_end = round_times(start, now_dttm)
    return abs_start, abs_end


# POST QUEUED ASYNC JOB
# pylint: disable=line-too-long
def post_queued_async_jobs(client, entity, report_name, report_type, window_start, report_config):
    LOGGER.info('Report: {}, Entity: {}, Type: {}, Date - POST ASYNC queued_job'.format(
        report_name, entity, report_type))
    # POST queued_job: asynchronous job
    queued_job = post_resource(report_name, entity, client, body=json.dumps(report_config))
    return queued_job


def get_async_results_urls(client, account_id, report_name, queued_job_ids):
    jobs_still_running = True # initialize
    j = 1 # job status check counter
    async_results_urls = []
    while len(queued_job_ids) > 0 and jobs_still_running and j <= 20:
        # Wait 15 sec for async reports to finish
        wait_sec = 15
        LOGGER.info('Report: {} - Waiting {} sec for async job to finish'.format(
            report_name, wait_sec))
        time.sleep(wait_sec)

        # GET async_job_status
        LOGGER.info('Report: {} - GET async_job_statuses'.format(report_name))
        async_job_statuses_path = 'stats/jobs/accounts/{account_id}'.replace(
            '{account_id}', account_id)
        async_job_statuses_params = {
            # What is the concurrent job_id limit?
            'job_ids': ','.join(map(str, queued_job_ids)),
            'count': 1000,
            'cursor': None
        }
        LOGGER.info('Report: {} - async_job_statuses GET URL: {}/{}/{}'.format(
            report_name, ADS_API_URL, API_VERSION, async_job_statuses_path))
        LOGGER.info('Report: {} - async_job_statuses params: {}'.format(
            report_name, async_job_statuses_params))
        async_job_statuses = get_resource('async_job_statuses', client, async_job_statuses_path, \
            async_job_statuses_params)

        jobs_still_running = False
        for async_job_status in async_job_statuses:
            job_status_dict = obj_to_dict(async_job_status)
            job_id = job_status_dict.get('id_str')
            job_status = job_status_dict.get('status')
            if job_status == 'PROCESSING':
                jobs_still_running = True
            elif job_status == 'SUCCESS':
                LOGGER.info('Report: {} - job_id: {}, finished running (SUCCESS)'.format(
                    report_name, job_id))
                job_results_url = job_status_dict.get('url')
                async_results_urls.append(job_results_url)
                # LOGGER.info('job_results_url = {}'.format(job_results_url)) # COMMENT OUT
                # Remove job_id from queued_job_ids
                queued_job_ids.remove(job_id)
            # End: async_job_status in async_job_statuses
        j = j + 1 # increment job status check counter
        # End: async_job_status in async_job_statuses
    return async_results_urls


def sync_report(client,
                catalog,
                state,
                start_date,
                report_name,
                report_config,
                tap_config,
                entity):

    # PROCESS:
    # Outer-outer loop (in sync): loop through accounts
    # Outer loop (in sync): loop through reports selected in catalog
    #   Each report definition: name, entity, dimensions
    #
    # For each Report:
    # 1. Determine start/end dates and date windows (rounded, limited, timezone);
    #     Loop through date windows from bookmark datetime to current datetime.
    # 2. POST ASYNC Job to Queue to get queued_job_id
    # 3. GET ASYNC Job Statuses and Download URLs (when complete)
    # 4. Download Data from URLs and Sync data to target
    report_type = report_config.get('type')
    advertiserIds = tap_config.get('advertiserIds')
    LOGGER.info('Report: {}, Entity: {}, Type: {}, Dimensions: {}'.format(
        report_name, entity, report_type, report_config.get('dimensions')))

    # Bookmark datetimes
    last_datetime = get_bookmark(state, report_name, start_date)
    last_dttm = strptime_to_utc(last_datetime)
    max_bookmark_value = last_datetime

    # Get absolute start and end times
    attribution_window = int(tap_config.get('attribution_window', '1'))
    abs_start, abs_end = get_absolute_start_end_time(
        'day', last_dttm, attribution_window)

    # Initialize date window
    date_window_size = 1
    window_start = abs_start
    window_end = (abs_start + timedelta(days=date_window_size))
    window_start_rounded = None
    window_end_rounded = None
    if window_end > abs_end:
        window_end = abs_end

    queued_reports = {}

    # DATE WINDOW LOOP
    while window_start != abs_end:
        window_entity_id_sets = []
        window_start_rounded, window_end_rounded = round_times(window_start, window_end)
        window_start_str = window_start_rounded.strftime('%Y%m%d')
        window_end_str = window_end_rounded.strftime('%Y%m%d')

        LOGGER.info('Report: {} - Date window: {} to {}'.format(
            report_name, window_start_str, window_end_str))

        dimensions = report_config.get('dimensions', REPORT_TYPE_DIMENSION_METRICS.get(report_type).get('DIMENSIONS'))
        metrics = ','.join(REPORT_TYPE_DIMENSION_METRICS.get(report_type).get('METRICS'))

        REPORT_CONFIG = {
            'reportDate': window_start_str,
            'format': 'JSON',
            'type': report_config.get('type'),
            'dimensions': dimensions,
            'metrics': metrics
        }

        if advertiserIds:
            REPORT_CONFIG['advertiserIds'] = advertiserIds

        job_result = post_queued_async_jobs(client, entity, report_name,
                                            report_type, window_start=window_start_str,
                                            report_config=REPORT_CONFIG)
        
        queued_reports[job_result.get('reportId')] = {
            "job_result": job_result,
            "report_config": REPORT_CONFIG
        }

        window_start = window_start + timedelta(days=date_window_size)

    # ASYNC report POST requests
    queued_job_ids = list(queued_reports)

    # Get stream_metadata from catalog (for Transformer masking and validation below)
    stream = catalog.get_stream(report_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    # ASYNC RESULTS DOWNLOAD / PROCESS LOOP
    total_records = 0
    # queued_job_ids = 
    for key in queued_reports:

        if report_is_ready(queued_reports.get('key')):
            async_data = get_async_data(report_name, client, queued_reports.get('key'))
            # LOGGER.info('async_data = {}'.format(async_data)) # COMMENT OUT

            # time_extracted: datetime when the data was extracted from the API
            time_extracted = utils.now()

            # TRANSFORM REPORT DATA
            transformed_data = []
            transformed_data = transform_report(report_name, async_data, account_id)
            # LOGGER.info('transformed_data = {}'.format(transformed_data)) # COMMENT OUT
            if transformed_data is None or transformed_data == []:
                LOGGER.info('Report: {} - NO TRANSFORMED DATA for URL: {}'.format(
                    report_name, async_results_url))

            # PROCESS RESULTS TO TARGET RECORDS
            with metrics.record_counter(report_name) as counter:
                for record in transformed_data:
                    # Transform record with Singer Transformer

                    # Evalueate max_bookmark_value
                    end_time = record.get('end_time') # String
                    end_dttm = strptime_to_utc(end_time) # Datetime
                    max_bookmark_dttm = strptime_to_utc(max_bookmark_value) # Datetime
                    if end_dttm > max_bookmark_dttm: # Datetime comparison
                        max_bookmark_value = end_time # String

                    with Transformer() as transformer:
                        transformed_record = transformer.transform(
                            record,
                            schema,
                            stream_metadata)

                        write_record(report_name, transformed_record, time_extracted=time_extracted)
                        counter.increment()

            # Increment total_records
            total_records = total_records + counter.value
            # End: for async_results_url in async_results_urls

    # Update the state with the max_bookmark_value for the stream
    write_bookmark(state, report_name, max_bookmark_value)

    return total_records
    # End sync_report

# Sync - main function to loop through select streams to sync_endpoints and sync_reports
def sync(client, config, catalog, state):
    # Get config parameters
    entity_list = config.get('entities').replace(' ', '').split(',')
    start_date = config.get('start_date')
    reports = config.get('reports', [])

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('Last/Currently Syncing Stream: {}'.format(last_stream))

    # Get ALL selected streams from catalog
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('Sync Selected Streams: {}'.format(selected_streams))
    if not selected_streams:
        return

    # Get list of report streams to sync (from config and catalog)
    report_streams = []
    for report in reports:
        report_name = report.get('name')
        if report_name in selected_streams:
            report_streams.append(report_name)
    LOGGER.info('Sync Report Streams: {}'.format(report_streams))

    # ENTITY OUTER LOOP
    for entity in entity_list:
        LOGGER.info('Entity: {} - START Syncing'.format(entity))

        # REPORT STREAMS LOOP
        for report in reports:
            report_name = report.get('name')
            # if report_name in report_streams:
            update_currently_syncing(state, report_name)

            LOGGER.info('Report: {} - START Syncing for Entity: {}'.format(
                report_name, entity))

            # Write schema and log selected fields for stream
            write_schema(catalog, report_name)

            selected_fields = get_selected_fields(catalog, report_name)
            LOGGER.info('Report: {} - selected_fields: {}'.format(
                report_name, selected_fields))

            total_records = sync_report(client=client,
                                        catalog=catalog,
                                        state=state,
                                        start_date=start_date,
                                        report_name=report_name,
                                        report_config=report,
                                        tap_config=config,
                                        entity=entity)

            # pylint: disable=line-too-long
            LOGGER.info('Report: {} - FINISHED Syncing for Entity: {}, Total Records: {}'.format(
                report_name, entity, total_records))
            # pylint: enable=line-too-long
            update_currently_syncing(state, None)

        LOGGER.info('Entity: {} - FINISHED Syncing'.format(entity))
