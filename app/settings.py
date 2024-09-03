import os

CAMPUSES = os.environ.get('CAMPUSES')
LOCAL = os.environ.get('LOCAL', False)
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

S3_BUCKET = os.environ.get('S3_BUCKET')