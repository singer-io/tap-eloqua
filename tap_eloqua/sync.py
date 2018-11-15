import re
import time
import random
from datetime import datetime, timedelta

import pendulum
import singer
from singer import metrics, metadata, Transformer, UNIX_SECONDS_INTEGER_DATETIME_PARSING

from tap_eloqua.schema import (
    PKS,
    BUILT_IN_BULK_OBJECTS,
    ACTIVITY_TYPES,
    get_schemas,
    activity_type_to_stream
)

LOGGER = singer.get_logger()

MIN_RETRY_INTERVAL = 2 # 10 seconds
MAX_RETRY_INTERVAL = 300 # 5 minutes
MAX_RETRY_ELAPSED_TIME = 3600 # 1 hour

def next_sleep_interval(previous_sleep_interval):
    min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
    max_interval = previous_sleep_interval * 2 or MIN_RETRY_INTERVAL
    return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))

def get_bookmark(state, stream, default):
    return (
        state
        .get('bookmarks', {})
        .get('visitors', default)
    )

def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    singer.write_state(state)

def write_schema(catalog, stream_id):
    stream = catalog.get_stream(stream_id)
    schema = stream.schema.to_dict()
    key_properties = PKS[stream_id]
    singer.write_schema(stream_id, schema, key_properties)

def persist_records(catalog, stream_id, records):
    stream = catalog.get_stream(stream_id)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)
    with metrics.record_counter(stream_id) as counter:
        for record in records:
            with Transformer(
                integer_datetime_fmt=UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
                record = transformer.transform(record,
                                               schema,
                                               stream_metadata)
            singer.write_record(stream_id, record)
            counter.increment()

def transform_export_row(row):
    out = {}
    for field, value in row.items():
        if value == '':
            value = None
        out[field] = value
    return out

def stream_export(client, catalog, stream_name, sync_id):
    LOGGER.info('{} - Pulling export results - {}'.format(stream_name, sync_id))

    write_schema(catalog, stream_name)

    limit = 50000
    offset = 0
    has_true = True
    while has_true:
        data = client.get(
            '/api/bulk/2.0/syncs/{}/data'.format(sync_id),
            params={
                'limit': limit,
                'offset': offset
            },
            endpoint='export_data')
        has_true = data['hasMore']
        offset += limit

        if 'items' in data and data['items']:
            records = map(transform_export_row, data['items'])
            persist_records(catalog, stream_name, records)

def sync_bulk_obj(client, catalog, state, start_date, stream_name, activity_type=None):
    LOGGER.info('{} - Starting export'.format(stream_name))

    stream = catalog.get_stream(stream_name)

    fields = {}
    for meta in stream.metadata:
        if meta['breadcrumb']:
            field_name = meta['breadcrumb'][1]
            fields[field_name] = meta['metadata']['tap-eloqua.statement']

    params = {
        'name': 'Singer Sync - ' + datetime.utcnow().isoformat(),
        'fields': fields,
        # 'filter': ,
        # 'autoDeleteDuration': (datetime.utcnow() + timedelta(hours=6)).isoformat(),
        # 'areSystemTimestampsInUTC': True
    }

    if activity_type:
        params['filter'] = "'{{Activity.Type}}'='" + activity_type + "'"

    if activity_type:
        url_obj = 'activities'
    else:
        url_obj = stream_name

    data = client.post(
        '/api/bulk/2.0/{}/exports'.format(url_obj),
        json=params,
        endpoint='export_create_def')

    data = client.post(
        '/api/bulk/2.0/syncs',
        json={
            'syncedInstanceUri': data['uri']
        },
        endpoint='export_create_sync')

    sync_id = re.match(r'/syncs/([0-9]+)', data['uri']).groups()[0]

    LOGGER.info('{} - Created export - {}'.format(stream_name, sync_id))

    sleep = 0
    start_time = time.time()
    while True:
        data = client.get(
            '/api/bulk/2.0/syncs/{}'.format(sync_id),
            endpoint='export_sync_poll')

        status = data['status']
        if status == 'success' or status == 'active':
            stream_export(client, catalog, stream_name, sync_id)
            break
        elif status != 'pending':
            message = '{} - status: {}, exporting failed'.format(
                    stream_name,
                    status)
            LOGGER.error(message)
            raise Exception(message)
        elif (time.time() - start_time) > MAX_RETRY_ELAPSED_TIME:
            message = '{} - export deadline exceeded ({} secs)'.format(
                    stream_name,
                    MAX_RETRY_ELAPSED_TIME)
            LOGGER.error(message)
            raise Exception(message)

        sleep = next_sleep_interval(sleep)
        LOGGER.info('{} - status: {}, sleeping for {} seconds'.format(
                    stream_name,
                    status,
                    sleep))
        time.sleep(sleep)

def sync_campaigns(client, catalog, state, start_date):
    write_schema(catalog, 'campaigns')

    last_campaign_raw = get_bookmark(state, 'campaigns', start_date)
    last_campaign = pendulum.parse(last_campaign_raw).to_datetime_string()
    search = "updatedAt>='{}'".format(last_campaign)

    page = 1
    count = 1000
    while True:
        data = client.get(
            '/api/REST/2.0/assets/campaigns',
            params={
                'count': count,
                'page': page,
                'depth': 'complete',
                'orderBy': 'updatedAt',
                'search': search
            },
            endpoint='campaigns')
        page += 1
        records = data['elements']

        persist_records(catalog, 'campaigns', records)

        if records:
            max_updated_at = pendulum.from_timestamp(
                int(records[-1]['updatedAt'])).to_iso8601_string()
            write_bookmark(state, 'campaigns', max_updated_at)

        if len(records) < count:
            break

def sync_visitors(client, catalog, state, start_date):
    write_schema(catalog, 'visitors')

    last_visit_raw = get_bookmark(state, 'visitors', start_date)
    last_visit = pendulum.parse(last_visit_raw).to_datetime_string()
    search = "v_LastVisitDateAndTime>='{}'".format(last_visit)

    page = 1
    count = 1000
    while True:
        data = client.get(
            '/api/REST/2.0/data/visitors',
            params={
                'count': count,
                'page': page,
                'depth': 'complete',
                'orderBy': 'v_LastVisitDateAndTime',
                'search': search
            },
            endpoint='visitors')
        page += 1
        records = data['elements']

        persist_records(catalog, 'visitors', records)

        if records:
            max_visit = pendulum.from_timestamp(
                records[-1]['v_LastVisitDateAndTime']).to_iso8601_string()
            write_bookmark(state, 'visitors', max_visit)

        if len(records) < count:
            break

def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)

def sync(client, catalog, state, start_date):
    selected_streams = get_selected_streams(catalog)

    if not selected_streams:
        return

    for bulk_object in BUILT_IN_BULK_OBJECTS:
        if bulk_object in selected_streams:
            sync_bulk_obj(client,
                          catalog,
                          state,
                          start_date,
                          bulk_object)

    for activity_type in ACTIVITY_TYPES:
        stream_name = activity_type_to_stream(activity_type)
        if stream_name in selected_streams:
            sync_bulk_obj(client,
                          catalog,
                          state,
                          start_date,
                          stream_name,
                          activity_type=activity_type)

    if 'visitors' in selected_streams:
        sync_visitors(client, catalog, state, start_date)

    if 'campaigns' in selected_streams:
        sync_campaigns(client, catalog, state, start_date)
