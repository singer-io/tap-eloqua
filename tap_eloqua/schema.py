import re
import os
import json

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

QUERY_LANGUAGE_MAP = {
    'accounts': 'Account',
    'contacts': 'Contact'
}

# https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAB/Developers/BulkAPI/Reference/Bulk%20languages/eloqua-markup-language-v3.htm

BASE_SYSTEM_FIELD = {
    'Id': {
        'type': 'string'
    },
    'CreatedAt': {
        'type': 'string',
        'format': 'date-time'
    }
}

BULK_SYSTEM_FIELDS = {
    **BASE_SYSTEM_FIELD,
    'UpdatedAt': {
        'type': 'string',
        'format': 'date-time'
    }
}

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def activity_type_to_stream(activity_type):
    return 'activity_' + camel_to_snake(activity_type)

PKS = {
    'assets': ['id'],
    'campaigns': ['id'],
    'emails': ['id'],
    'forms': ['id'],
    'visitors': []
}

for bulk_object in BUILT_IN_BULK_OBJECTS:
    PKS[bulk_object] = ['Id']

for activity_type in ACTIVITY_TYPES:
    PKS[activity_type_to_stream(activity_type)] = []

def get_pk(stream_name):
    if stream_name in PKS:
        return PKS[stream_name]
    return ['Id']

def get_type(eloqua_field):
    eloqua_type = eloqua_field['dataType']

    json_type = 'string'
    json_format = None
    if eloqua_type == 'date':
        json_format = 'date-time'
    elif eloqua_type == 'number':
        json_type = 'number'

    internal_name = eloqua_field['internalName']
    name_last_two = internal_name[-2:]
    if internal_name == 'Duration' or \
       ((name_last_two == 'Id' or name_last_two == 'ID') and json_type == 'number'):
        json_type = 'string'

    return ['null', json_type], json_format

def to_meta(inclusion, statement, field_name):
    return {
        'metadata': {
            'inclusion': inclusion,
            'tap-eloqua.statement': statement
        },
        'breadcrumb': ['properties', field_name]
    }

def get_bulk_schema(client,
                    stream_name,
                    path,
                    system_fields,
                    query_language_name=None,
                    activity_type=None,
                    object_id=None):
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

    if not query_language_name:
        if activity_type is not None:
            query_language_name = 'Activity'
        elif stream_name in QUERY_LANGUAGE_MAP:
            query_language_name = QUERY_LANGUAGE_MAP[stream_name]

    metadata.append({
        'metadata': {
            'tap-eloqua.id': object_id,
            'tap-eloqua.query-language-name': query_language_name
        },
        'breadcrumb': []
    })

    pk = get_pk(stream_name)

    for prop, json_schema in system_fields.items():
        properties[prop] = json_schema

        if prop in pk:
            inclusion = 'automatic'
        else:
            inclusion = 'available'

        statement = (
            '{{' +
            query_language_name +
            '.' +
            prop +
            '}}'
        )

        meta = to_meta(inclusion, statement, prop)
        metadata.append(meta)

    for eloqua_field in data['items']:
        field_name = eloqua_field['internalName']

        if field_name in properties:
            if field_name not in system_fields:
                raise Exception('Duplicate field detected: {}'.format(field_name))
            continue

        json_type, format = get_type(eloqua_field)
        json_schema = {
            'type': json_type
        }

        if format:
            json_schema['format'] = format

        properties[field_name] = json_schema

        meta = to_meta('available', eloqua_field['statement'], field_name)

        if 'uri' in eloqua_field and eloqua_field['uri'] != '':
            field_id = re.match(r'.*/fields/([0-9]+)', eloqua_field['uri']).groups()[0]
            meta['metadata']['tap-eloqua.id'] = field_id

        metadata.append(meta)

    schema = {
        'properties': properties,
        'additionalProperties': False,
        'type': 'object'
    }

    return schema, metadata

def get_bulk_obj_schema(client, stream_name, obj_name, system_fields, **kwargs):
    return get_bulk_schema(client,
                           stream_name,
                           '/api/bulk/2.0/{}/fields'.format(obj_name),
                           system_fields,
                           **kwargs)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_static_schemas():
    global SCHEMAS, FIELD_METADATA

    schemas_path = get_abs_path('schemas')

    file_names = [f for f in os.listdir(schemas_path)
                  if os.path.isfile(os.path.join(schemas_path, f))]

    for file_name in file_names:
        stream_name = file_name[:-5]
        with open(os.path.join(schemas_path, file_name)) as data_file:
            schema = json.load(data_file)
            
        SCHEMAS[stream_name] = schema
        pk = PKS[stream_name]

        metadata = []
        for prop, json_schema in schema['properties'].items():
            if prop in pk:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            metadata.append({
                'metadata': {
                    'inclusion': inclusion
                },
                'breadcrumb': ['properties', prop]
            })
        FIELD_METADATA[stream_name] = metadata

def get_schemas(client):
    global SCHEMAS, FIELD_METADATA

    if SCHEMAS:
        return SCHEMAS, FIELD_METADATA

    SCHEMAS = {}
    FIELD_METADATA = {}

    for bulk_object in BUILT_IN_BULK_OBJECTS:
        json_schema, metadata = get_bulk_obj_schema(client,
                                                    bulk_object,
                                                    bulk_object,
                                                    BULK_SYSTEM_FIELDS)
        SCHEMAS[bulk_object] = json_schema
        FIELD_METADATA[bulk_object] = metadata

    for activity_type in ACTIVITY_TYPES:
        stream_name = activity_type_to_stream(activity_type)
        json_schema, metadata = get_bulk_obj_schema(client,
                                                    stream_name,
                                                    'activities',
                                                    BASE_SYSTEM_FIELD,
                                                    activity_type=activity_type)
        SCHEMAS[stream_name] = json_schema
        FIELD_METADATA[stream_name] = metadata

    ## TODO: pagination
    data = client.get('/api/bulk/2.0/customObjects')

    for custom_obj in data['items']:
        groups = re.match(r'/customObjects/([0-9]+)',
                          custom_obj['uri']).groups()
        object_id = groups[0]

        ## TODO: more normalization?
        stream_name = (
            custom_obj['name']
            .strip()
            .lower()
            .replace(' ', '_')
            .replace('-', '_')
        )

        query_language_name = 'CustomObject[{}]'.format(object_id)
        json_schema, metadata = get_bulk_schema(
            client,
            stream_name,
            '/api/bulk/2.0/customObjects/{}/fields'.format(object_id),
            BASE_SYSTEM_FIELD,
            query_language_name=query_language_name,
            object_id=object_id)

        SCHEMAS[stream_name] = json_schema
        FIELD_METADATA[stream_name] = metadata

    get_static_schemas()

    return SCHEMAS, FIELD_METADATA
