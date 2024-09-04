import os

CAMPUSES = os.environ.get('CAMPUSES')

METADATA = os.environ.get('NUXEO_EXTENT_STATS_METADATA')
REPORTS = os.environ.get('NUXEO_EXTENT_STATS_REPORTS')
TEMP = os.environ.get('NUXEO_EXTENT_STATS_LOCAL_TEMPDIR')

NUXEO_API_ES_ENDPOINT_BROKEN = os.environ.get('NUXEO_API_ES_ENDPOINT_BROKEN', False)
NUXEO_TOKEN = os.environ.get('NUXEO_TOKEN')
NUXEO_API = os.environ.get('NUXEO_API')
NUXEO_REQUEST_HEADERS = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-NXDocumentProperties": "*",
                "X-NXRepository": "default",
                "X-Authentication-Token": NUXEO_TOKEN
                }