import os
import json
import boto3

from nuxeoextent import metadatafetcher

DEBUG = os.environ.get('DEBUG', False)

def lambda_handler(payload, context):
    if DEBUG:
        payload = json.loads(payload)

    fetcher = metadatafetcher.Fetcher(payload)
    fetcher.fetch_page()
    next_page = fetcher.json()

    if next_page:
        if DEBUG:
            lambda_handler(next_page, {})
        else:
            lambda_client = boto3.client('lambda', region_name="us-west-2",)
            lambda_client.invoke(
                FunctionName="fetch-nuxeo-extent-metadata",
                InvocationType="Event",  # invoke asynchronously
                Payload=next_page.encode('utf-8')
            )


    return {
        'statusCode': 200,
        'body': json.dumps(payload)
    }