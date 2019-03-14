import re
import time
import random
from datetime import datetime, timedelta

import pendulum
import singer
from singer import metrics, metadata, Transformer, UNIX_SECONDS_INTEGER_DATETIME_PARSING
from requests.exceptions import HTTPError

from tap_eloqua.schema import (
    BUILT_IN_BULK_OBJECTS,
    ACTIVITY_TYPES,
    get_schemas,
    activity_type_to_stream
)

LOGGER = singer.get_logger()

MIN_RETRY_INTERVAL = 2 # 2 seconds
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
        .get(stream, default)
    )

def get_bulk_bookmark(state, stream):
    bookmark = get_bookmark(state, stream, {})
    if isinstance(bookmark, str):
        return {
            'datetime': bookmark
        }
    return bookmark

def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    singer.write_state(state)

def write_bulk_bookmark(state, stream_name, sync_id, offset, max_updated_at):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream_name] = {
        'sync_id': sync_id,
        'offset': offset,
        'datetime': max_updated_at
    }
    singer.write_state(state)

def write_schema(catalog, stream_id):
    stream = catalog.get_stream(stream_id)
    schema = stream.schema.to_dict()
    singer.write_schema(stream_id, schema, stream.key_properties)

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

def stream_export(client,
                  state,
                  catalog,
                  stream_name,
                  sync_id,
                  updated_at_field,
                  bulk_page_size,
                  bookmark_datetime,
                  offset=0):
    LOGGER.info('{} - Pulling export results - {}'.format(stream_name, sync_id))

    write_schema(catalog, stream_name)

    has_more = True
    max_updated_at = None
    while has_more:
        LOGGER.info('{} - Paginating export results - offset: {}, limit: {}'.format(
            stream_name,
            offset,
            bulk_page_size))

        write_bulk_bookmark(state, stream_name, sync_id, offset, bookmark_datetime)

        data = client.get(
            '/api/bulk/2.0/syncs/{}/data'.format(sync_id),
            params={
                'limit': bulk_page_size,
                'offset': offset
            },
            endpoint='export_data')
        has_more = data['hasMore']
        offset += bulk_page_size

        if 'items' in data and data['items']:
            records = map(transform_export_row, data['items'])
            persist_records(catalog, stream_name, records)

            max_page_updated_at = max(map(lambda x: x[updated_at_field], data['items']))
            if max_updated_at is None or max_page_updated_at > max_updated_at:
                max_updated_at = max_page_updated_at

    final_datetime = max_updated_at or bookmark_datetime
    write_bulk_bookmark(state, stream_name, None, None, final_datetime)

    return final_datetime

def sync_bulk_obj(client, catalog, state, start_date, stream_name, bulk_page_size, activity_type=None):
    LOGGER.info('{} - Starting export'.format(stream_name))

    stream = catalog.get_stream(stream_name)
    if activity_type:
        updated_at_field = 'CreatedAt'
    else:
        updated_at_field = 'UpdatedAt'

    last_bookmark = get_bulk_bookmark(state, stream_name)
    last_date_raw = last_bookmark.get('datetime', start_date)
    last_date = pendulum.parse(last_date_raw).to_datetime_string()
    last_sync_id = last_bookmark.get('sync_id')
    last_offset = last_bookmark.get('offset')

    if last_sync_id:
        LOGGER.info('{} - Resuming previous export: {}'.format(stream_name, last_sync_id))
        try:
            last_date = stream_export(client,
                                      state,
                                      catalog,
                                      stream_name,
                                      last_sync_id,
                                      updated_at_field,
                                      bulk_page_size,
                                      last_date,
                                      offset=last_offset)
        except HTTPError as e:
            if e.response.status_code in [404, 410]:
                LOGGER.info('{} - Previous export expired: {}'.format(stream_name, last_sync_id))
            else:
                raise

    fields = {}
    obj_meta = None
    for meta in stream.metadata:
        if not meta['breadcrumb']:
            obj_meta = meta['metadata']
        elif meta['metadata'].get('selected', True) or \
             meta['metadata'].get('inclusion', 'available') == 'automatic':
            field_name = meta['breadcrumb'][1]
            fields[field_name] = meta['metadata']['tap-eloqua.statement']

    num_fields = len(fields.values())
    if num_fields > 250:
        LOGGER.error('{} - Exports can only have 250 fields selected. {} are selected.'.format(
            stream_name, num_fields))
    else:
        LOGGER.info('{} - Syncing {} fields'.format(stream_name, num_fields))

    language_obj = obj_meta['tap-eloqua.query-language-name']

    _filter = "'{{" + language_obj + "." + updated_at_field + "}}' >= '" + last_date + "'"

    if activity_type is not None:
        _filter += " AND '{{Activity.Type}}' = '" + activity_type + "'"

    params = {
        'name': 'Singer Sync - ' + datetime.utcnow().isoformat(),
        'fields': fields,
        'filter': _filter,
        'areSystemTimestampsInUTC': True
    }

    if activity_type:
        url_obj = 'activities'
    elif obj_meta['tap-eloqua.id']:
        url_obj = 'customObjects/' + obj_meta['tap-eloqua.id']
    else:
        url_obj = stream_name

    with metrics.job_timer('bulk_export'):
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
            if status == 'success':
                break
            elif status not in ['pending', 'active']:
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

    stream_export(client,
                  state,
                  catalog,
                  stream_name,
                  sync_id,
                  updated_at_field,
                  bulk_page_size,
                  last_date)

def sync_static_endpoint(client, catalog, state, start_date, stream_id, path, updated_at_col):
    write_schema(catalog, stream_id)

    last_date_raw = get_bookmark(state, stream_id, start_date)
    last_date = pendulum.parse(last_date_raw).to_datetime_string()
    search = "{}>='{}'".format(updated_at_col, last_date)

    page = 1
    count = 1000
    while True:
        LOGGER.info('Syncing {} since {} - page {}'.format(stream_id, last_date, page))
        data = client.get(
            '/api/REST/2.0/{}'.format(path),
            params={
                'count': count,
                'page': page,
                'depth': 'complete',
                'orderBy': updated_at_col,
                'search': search
            },
            endpoint=stream_id)
        page += 1
        records = data.get('elements', [])

        persist_records(catalog, stream_id, records)

        if records:
            max_updated_at = pendulum.from_timestamp(
                int(records[-1][updated_at_col])).to_iso8601_string()
            write_bookmark(state, stream_id, max_updated_at)

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

def update_current_stream(state, stream_name):
    state['current_stream'] = stream_name
    singer.write_state(state)

def should_sync_stream(selected_streams, last_stream, stream_name):
    if last_stream == stream_name or last_stream is None:
        if last_stream is not None:
            last_stream = None
        if stream_name in selected_streams:
            return True, last_stream
    return False, last_stream

def get_custom_obj_streams(catalog):
    custom_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('tap-eloqua.id'):
            custom_streams.add(stream.tap_stream_id)
    return list(custom_streams)

def sync(client, catalog, state, start_date, bulk_page_size):
    selected_streams = get_selected_streams(catalog)

    if not selected_streams:
        return

    last_stream = state.get('current_stream')

    for bulk_object in BUILT_IN_BULK_OBJECTS:
        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        bulk_object)
        if should_stream:
            update_current_stream(state, bulk_object)
            sync_bulk_obj(client,
                          catalog,
                          state,
                          start_date,
                          bulk_object,
                          bulk_page_size)

    for activity_type in ACTIVITY_TYPES:
        stream_name = activity_type_to_stream(activity_type)
        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        stream_name)
        if should_stream:
            update_current_stream(state, stream_name)
            sync_bulk_obj(client,
                          catalog,
                          state,
                          start_date,
                          stream_name,
                          bulk_page_size,
                          activity_type=activity_type)

    for stream_name in get_custom_obj_streams(catalog):
        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        stream_name)
        if should_stream:
            update_current_stream(state, stream_name)
            sync_bulk_obj(client,
                          catalog,
                          state,
                          start_date,
                          stream_name,
                          bulk_page_size)

    static_endpoints = [
        {
            'stream_id': 'visitors',
            'path': 'data/visitors',
            'updated_at_col': 'V_LastVisitDateAndTime'
        },
        {
            'stream_id': 'campaigns',
            'path': 'assets/campaigns',
            'updated_at_col': 'updatedAt'
        },
        {
            'stream_id': 'emails',
            'path': 'assets/emails',
            'updated_at_col': 'updatedAt'
        },
        {
            'stream_id': 'forms',
            'path': 'assets/forms',
            'updated_at_col': 'updatedAt'
        },
        {
            'stream_id': 'assets',
            'path': 'assets/externals',
            'updated_at_col': 'updatedAt'
        }
    ]

    for static_endpoint in static_endpoints:
        stream_id = static_endpoint['stream_id']
        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        stream_id)
        if should_stream:
            update_current_stream(state, stream_id)
            path = static_endpoint['path']
            updated_at_col = static_endpoint['updated_at_col']
            sync_static_endpoint(client,
                                 catalog,
                                 state,
                                 start_date,
                                 stream_id,
                                 path,
                                 updated_at_col)

    update_current_stream(state, None)
