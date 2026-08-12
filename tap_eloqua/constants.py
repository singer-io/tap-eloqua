"""Module-level constants shared across tap_eloqua modules."""

STATIC_ENDPOINTS = [
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
    },
    {
        'stream_id': 'emailGroups',
        'path': 'assets/email/groups',
        'updated_at_col': 'updatedAt'
    },
]
