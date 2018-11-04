import re

SCHEMAS = None
FIELD_METADATA = None

ACTIVITY_TYPES = [
    'EmailOpen',
    'EmailClickthrough',
    'EmailSend',
    'Subscribe',
    'Unsubscribe',
    'Bounceback',
    'WebVisit',
    'PageView',
    'FormSubmit'
]

BUILT_IN_BULK_OBJECTS = [
    'accounts',
    'contacts'
]

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def activity_type_to_stream(activity_type):
    return 'activity_' + camel_to_snake(activity_type)

PKS = {}

for bulk_object in BUILT_IN_BULK_OBJECTS:
    PKS[bulk_object] = ['id']

for activity_type in ACTIVITY_TYPES:
    PKS[activity_type_to_stream(activity_type)] = 'Id'

def get_type(eloqua_type):
    if eloqua_type == 'date':
        return 'string', 'date-time'
    if eloqua_type == 'number':
        return 'number', None
    return 'string', None

def get_bulk_schema(client, path, activity_type=None):
    params = {}
    if activity_type:
        params['activityType'] = activity_type

    ## TODO: pagination
    data = client.get(
        path,
        params=params,
        endpoint='bulk_fields')

    properties = {}
    metadata = []
    for eloqua_field in data['items']:
        field_name = eloqua_field['internalName']
        json_type, format = get_type(eloqua_field['dataType'])
        json_schema = {
            'type': json_type
        }

        if format:
            json_schema['format'] = format

        properties[field_name] = json_schema

        meta = {
            'metadata': {
                'inclusion': 'available',
                'tap-eloqua.statement': eloqua_field['statement']
            },
            'breadcrumb': ['properties', field_name]
        }

        if 'uri' in eloqua_field and eloqua_field['uri'] != '':
            field_id = re.match(r'.*/fields/([0-9]+)', eloqua_field['uri']).groups()[0]
            meta['metadata']['tap-eloqua.id'] = field_id

        metadata.append(meta)

    schema = {
        'properties': properties,
        'type': 'object'
    }

    return schema, metadata

def get_bulk_obj_schema(client, obj_name, **kwargs):
    return get_bulk_schema(client,
                           '/api/bulk/2.0/{}/fields'.format(obj_name),
                           **kwargs)

def get_schemas(client):
    global SCHEMAS, FIELD_METADATA

    if SCHEMAS:
        return SCHEMAS, FIELD_METADATA

    SCHEMAS = {}
    FIELD_METADATA = {}

    for bulk_object in BUILT_IN_BULK_OBJECTS:
        json_schema, metadata = get_bulk_obj_schema(client, bulk_object)
        SCHEMAS[bulk_object] = json_schema
        FIELD_METADATA[bulk_object] = metadata

    for activity_type in ACTIVITY_TYPES:
        json_schema, metadata = get_bulk_obj_schema(client,
                                                    'activities',
                                                    activity_type=activity_type)
        stream_name = activity_type_to_stream(activity_type)
        SCHEMAS[stream_name] = json_schema
        FIELD_METADATA[stream_name] = metadata

    ## TODO: pagination
    data = client.get('/api/bulk/2.0/customObjects')

    for custom_obj in data['items']:
        groups = re.match(r'/customObjects/([0-9]+)',
                          custom_obj['uri']).groups()
        object_id = groups[0]

        json_schema, metadata = get_bulk_schema(
            client,
            '/api/bulk/2.0/customObjects/{}/fields'.format(object_id))

        ## TODO: add id column?

        ## TODO: more normalization?
        stream_name = (
            custom_obj['name']
            .strip()
            .lower()
            .replace(' ', '_')
            .replace('-', '_')
        )

        SCHEMAS[stream_name] = json_schema
        FIELD_METADATA[stream_name] = metadata
        PKS[stream_name] = 'id'

    return SCHEMAS, FIELD_METADATA
