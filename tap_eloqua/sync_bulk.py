import re
import time
from datetime import datetime, timedelta

from singer import metrics, metadata, Transformer

from tap_eloqua.schema import get_schemas, PKS

def sync_bulk(client, catalog, state, start_date):
    stream = catalog.get_stream('accounts')
    schema = stream.schema.to_dict()
    #stream_metadata = metadata.to_map(stream.metadata)

    fields = {}
    for meta in stream.metadata:
        field_name = meta['breadcrumb'][1]
        fields[field_name] = meta['metadata']['tap-eloqua.statement']

    fields['Id'] = '{{Account.Id}}'
    fields['UpdatedAt'] = '{{Account.UpdatedAt}}'
    fields['CreatedAt'] = '{{Account.CreatedAt}}'

    data = client.post(
        '/api/bulk/2.0/accounts/exports',
        json={
            'name': 'Singer Account Sync - ' + datetime.utcnow().isoformat(),
            'fields': fields,
            # 'filter': ,
            # 'autoDeleteDuration': (datetime.utcnow() + timedelta(hours=6)).isoformat(),
            # 'areSystemTimestampsInUTC': True
        },
        endpoint='account_export_def')

    print(data)

    data = client.post(
        '/api/bulk/2.0/syncs',
        json={
            'syncedInstanceUri': data['uri']
        })

    print(data)

    sync_id = re.match(r'/syncs/([0-9]+)', data['uri']).groups()[0]

    notSynced = True
    status = None
    while notSynced:
        data = client.get('/api/bulk/2.0/syncs/{}'.format(sync_id))
        print(data)
        if data['status'] == 'pending':
            time.sleep(2)
        else:
            notSynced = False
            print(data['status'])
            status = data['status']

    if status == 'success':
        data = client.get('/api/bulk/2.0/syncs/{}/data'.format(sync_id))
        print(data)
