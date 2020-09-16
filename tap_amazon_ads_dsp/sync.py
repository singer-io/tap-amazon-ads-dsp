import json
import time
from collections import OrderedDict
from datetime import timedelta
from urllib.parse import urlparse

import singer
from singer import Transformer, metadata, metrics, utils
from singer.utils import strptime_to_utc
from tap_amazon_ads_dsp.client import stream_csv
from tap_amazon_ads_dsp.schema import DIMENSION_FIELDS, REPORT_STREAMS
from tap_amazon_ads_dsp.transform import transform_report

LOGGER = singer.get_logger()
DEFAULT_ATTRIBUTION_WINDOW = 14
DATE_WINDOW_SIZE = 1


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    LOGGER.info("Stream: {} - Writing schema".format(stream_name))
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.error(
            "Stream: {} - OS Error writing schema".format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name,
                                     record,
                                     time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error(
            "Stream: {} - OS Error writing record".format(stream_name))
        LOGGER.error("record: {}".format(record))
        raise err


def get_bookmark(state, stream, profile, default):
    # default only populated on initial sync
    if ((state is None) or ("bookmarks" not in state)
            or (stream not in state["bookmarks"])
            or (profile not in state["bookmarks"][stream])):
        return default
    return state.get("bookmarks", {}).get(stream).get(profile, default)


def write_bookmark(state, stream, profile, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    if stream not in state["bookmarks"]:
        state["bookmarks"][stream] = {}
    if profile not in state["bookmarks"][stream]:
        state["bookmarks"][stream][profile] = {}
    state["bookmarks"][stream][profile] = value
    LOGGER.info(
        "Stream: {}, Profile {} - Write state, bookmark value: {}".format(
            stream, profile, value))
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


def get_resource(stream_name, client, profile, job_id,):
    try:
        response = client.make_request(method="GET",
                                       profile=profile,
                                       job=job_id)
    except Exception as err:
        LOGGER.error("Stream: {} - ERROR: {}".format(stream_name, err))
        raise err
    response_body = response.json()
    return response_body


def post_resource(client, report_name, profile, body=None):
    try:
        response = client.make_request(method="POST",
                                       profile=profile,
                                       body=body)
    except Exception as err:
        LOGGER.error("Report: {} - ERROR: {}".format(report_name, err))
        raise err
    response_body = response.json()
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
            field = entry["breadcrumb"][1]
            if entry.get("metadata", {}).get("selected", False):
                selected_fields.append(field)
        except IndexError:
            pass
    return selected_fields


def remove_minutes_local(dttm, tzone):
    new_dttm = dttm.astimezone(tzone).replace(minute=0,
                                              second=0,
                                              microsecond=0)
    return new_dttm


def remove_hours_local(dttm):
    new_dttm = dttm.replace(hour=0, minute=0, second=0, microsecond=0)
    return new_dttm


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
# the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ("currently_syncing" in state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)
    LOGGER.info("Stream: {} - Currently Syncing".format(stream_name))


# Round time based to day
def round_times(start=None, end=None):
    start_rounded = None
    end_rounded = None
    # Round min_start, max_end to hours or dates
    start_rounded = remove_hours_local(start) - timedelta(days=1)
    end_rounded = remove_hours_local(end) + timedelta(days=1)
    return start_rounded, end_rounded


# Determine absolute start and end times w/ attribution_window constraint
# abs_start/end and window_start/end must be rounded to nearest hour or day (granularity)
def get_absolute_start_end_time(last_dttm, attribution_window):
    now_dttm = utils.now()
    delta_days = (now_dttm - last_dttm).days
    if delta_days < attribution_window:
        start = now_dttm - timedelta(days=attribution_window)
    elif delta_days > 89:
        start = now_dttm - timedelta(88)
        LOGGER.info(
            (f'Start date with attribution window exceeds max API history.'
             f'Setting start date to {start}')
        )
    else:
        start = last_dttm

    abs_start, abs_end = round_times(start, now_dttm)
    return abs_start, abs_end


# POST QUEUED ASYNC JOB
# pylint: disable=line-too-long
def post_queued_async_jobs(client, profile, report_name, report_config):
    LOGGER.info(
        "Report: {}, Profile: {}, Type: {}, Date - POST ASYNC queued_job".
        format(report_name, profile, report_config["type"]))
    # POST queued_job: asynchronous job
    queued_job = post_resource(client,
                               report_name,
                               profile,
                               body=json.dumps(report_config))
    return queued_job


def report_is_ready(stream, client, profile, job_id):
    job_status = get_resource(stream, client, profile, job_id)
    if job_status.get("status") == "SUCCESS":
        uri = None
        try:
            uri = urlparse(job_status.get("location"))
        except Exception:
            LOGGER.info(f"Found bad location URI {uri}")
            raise Exception
        return True, uri.geturl()
    return False, None


def to_epoch(dttm):
    epoch_time = strptime_to_utc("19700101")
    return int((dttm - epoch_time).total_seconds())

# pylint: disable=too-many-statements
def sync_report(client,
                catalog,
                state,
                start_date,
                report_name,
                report_config,
                tap_config,
                profile,
                selected_fields):

    # PROCESS:
    # Outer-outer loop (in sync): loop through accounts
    # Outer loop (in sync): loop through reports selected in catalog
    #   Each report definition: name, profile, dimensions
    #
    # For each Report:
    # 1. Determine start/end dates and date windows (rounded, limited, timezone);
    #     Loop through date windows from bookmark datetime to current datetime.
    # 2. POST ASYNC Job to Queue to get queued_job_id
    # 3. GET ASYNC Job Statuses and Download URLs (when complete)
    # 4. Download Data from URLs and Sync data to target
    report_type = report_config.get("type")
    advertiser_ids = tap_config.get("advertiserIds")
    LOGGER.info("Report: {}, Profile: {}, Type: {}, Dimensions: {}".format(
        report_name, profile, report_type, report_config.get("dimensions")))

    # Bookmark datetimes
    last_datetime = str(get_bookmark(state, report_name, profile, start_date))
    last_dttm = strptime_to_utc(last_datetime)

    # Get absolute start and end times
    attribution_window = int(
        tap_config.get("attribution_window", DEFAULT_ATTRIBUTION_WINDOW))
    abs_start, abs_end = get_absolute_start_end_time(last_dttm,
                                                     attribution_window)

    window_start = abs_start
    queued_reports = OrderedDict()

    # DATE WINDOW LOOP
    while window_start != abs_end:
        window_start_str = window_start.strftime("%Y%m%d")

        LOGGER.info("Report: {} - Date window: {}".format(
            report_name, window_start_str))

        # Dimensions for API request
        api_dimensions = report_config.get(
            "dimensions",
            REPORT_STREAMS.get(report_type).get("dimensions"))

        # Add selected metrics for API request
        selected_metrics = ""
        for selected in selected_fields:
            if selected not in DIMENSION_FIELDS:
                selected_metrics = (selected if not selected_metrics else
                                    selected_metrics + "," + selected)

        report_config = {
            "startDate": window_start_str,
            "endDate": window_start_str,
            "format": "CSV",
            "type": report_type.upper(),
            "timeUnit": REPORT_STREAMS.get(report_type).get("timeUnit"),
            "dimensions": api_dimensions,
            "metrics": selected_metrics,
        }

        if advertiser_ids:
            report_config["advertiserIds"] = advertiser_ids

        job_result = post_queued_async_jobs(
            client,
            profile,
            report_name,
            report_config,
        )

        queued_reports[job_result.get("reportId")] = {
            "job_result": job_result,
            "report_config": report_config
        }

        window_start = window_start + timedelta(days=DATE_WINDOW_SIZE)

    # ASYNC report POST requests
    queued_job_ids = list(queued_reports)

    # Get stream_metadata from catalog (for Transformer masking and validation below)
    stream = catalog.get_stream(report_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    total_records = 0

    # ASYNC RESULTS DOWNLOAD / PROCESS LOOP
    # - Reports endpoints returns SUCCESS and URI location when report is ready
    # - Process queued reports in order 
    # - Retry with exponential backoff limited to 512 sec
    max_bookmark_value = last_dttm
    job_retries = 0
    while queued_job_ids:
        job_id = queued_job_ids[0]

        report_date = (queued_reports.get(job_id).get("report_config").get(
                "startDate"))

        # Exponential backoff to maxiumum of 512 seconds
        wait_sec = 2**job_retries
        LOGGER.info(
            f"Job: {job_id}, Report: {report_name}, report date {report_date}: Retry - Waiting {wait_sec} sec for async job to finish"
        )
        time.sleep(wait_sec)

        ready, location = report_is_ready(stream,
                                          client,
                                          profile,
                                          job_id=job_id)

        if ready:            
            LOGGER.info(
                f"Job {job_id}, Report: {report_name}, report date {report_date} ready: retrieving location {location}"
            )
            time_extracted = utils.now()

            report_dttm = strptime_to_utc(report_date)
            report_type = (queued_reports.get(job_id).get("report_config").get(
                "type").lower())
            report_dimensions = (queued_reports.get(job_id).get(
                "report_config").get("dimensions"))

            # PROCESS RESULTS TO TARGET RECORDS
            with metrics.record_counter(report_name) as counter:
                for records in stream_csv(location):
                    if records:
                        transform_report(
                            schema,
                            report_type,
                            report_dttm,
                            report_dimensions,
                            records
                        )
                        # Transform record with Singer Transformer
                        with Transformer() as transformer:
                            for record in records:

                                # Evalueate max_bookmark_value
                                if report_dttm > max_bookmark_value:  # Datetime comparison
                                    max_bookmark_value = report_dttm

                                singer_transform_record = transformer.transform(
                                    record, schema, stream_metadata)

                                write_record(
                                    report_name,
                                    singer_transform_record,
                                    time_extracted=time_extracted,
                                )
                                counter.increment()
                    else:
                        break
                write_bookmark(state, report_name, profile,
                               max_bookmark_value.strftime('%Y-%m-%dT%H:%M:%S%z'))
                # Increment total_records
                total_records = total_records + counter.value
                job_retries = 0
            queued_job_ids.remove(job_id)
        else:
            # Exponential to limit of 512 seconds
            # Probably need a max tries and exit
            if job_retries < 9:
                job_retries += 1

    return total_records
    # End sync_report


# Sync - main function to loop through select streams to sync_endpoints and sync_reports
def sync(client, config, catalog, state):
    # Get config parameters
    profiles = config.get("profiles")
    if isinstance(profiles, str):
        profiles = profiles.replace(" ", "").split(",")

    start_date = config.get("start_date")
    reports = config.get("reports", [])
    if isinstance(reports, str):
        reports = json.loads(reports)

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("Last/Currently Syncing Stream: {}".format(last_stream))

    # Get ALL selected streams from catalog
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info("Sync Selected Streams: {}".format(selected_streams))
    if not selected_streams:
        return

    # Get list of report streams to sync (from config and catalog)
    report_streams = []
    for report in reports:
        report_name = report.get("name")
        if report_name in selected_streams:
            report_streams.append(report_name)
    LOGGER.info("Sync Report Streams: {}".format(report_streams))

    # PROFILE OUTER LOOP
    for profile in profiles:
        LOGGER.info("Profile: {} - START Syncing".format(profile))

        # REPORT STREAMS LOOP
        total_profile_records = 0
        for report in reports:
            report_name = report.get("name")
            # if report_name in report_streams:
            update_currently_syncing(state, report_name)

            LOGGER.info("Report: {} - START Syncing for Profile: {}".format(
                report_name, profile))

            # Write schema and log selected fields for stream
            write_schema(catalog, report_name)

            selected_fields = get_selected_fields(catalog, report_name)
            LOGGER.info("Report: {} - selected_fields: {}".format(
                report_name, selected_fields))

            total_records = sync_report(
                client=client,
                catalog=catalog,
                state=state,
                start_date=start_date,
                report_name=report_name,
                report_config=report,
                tap_config=config,
                profile=profile,
                selected_fields=selected_fields,
            )

            total_profile_records = total_profile_records + total_records
            # pylint: disable=line-too-long
            LOGGER.info(
                "Report: {} - FINISHED Syncing for Profile: {}, Total Records: {}"
                .format(report_name, profile, total_records))
            # pylint: enable=line-too-long
            update_currently_syncing(state, None)

        LOGGER.info("Profile: {} - FINISHED Syncing, Total Records: {}".format(
            profile, total_profile_records))
