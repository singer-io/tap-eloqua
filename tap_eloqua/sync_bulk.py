import re
import time
import random
from datetime import datetime, timedelta

import singer
from singer import metrics, metadata, Transformer

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
            with Transformer() as transformer:
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

    print(params)

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

def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)

def sync_bulk(client, catalog, state, start_date):
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
