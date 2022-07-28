import sys, os
import boto3
import json

BUCKET = os.environ.get('S3_BUCKET')

def report(path):
    # iterate over metadata stashed on S3 with prefix `path`
    # each line in each jsonl file is an item
    # for each doc: 
        # parent or child (parent if parent is org; child if parent is doc)
        # for each file:
            # size
            # type (content, aux, derivative)
    # get total number of parents
    # get total number of children
    # get total number of files
    prefix = path.lstrip('/asset-library/')
    prefix = f"metadata/{prefix}"

    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix
    )

    doc_count = 0
    for page in pages:

        for item in page['Contents']:
            response = s3_client.get_object(
                Bucket=BUCKET,
                Key=item['Key']
            )

            for line in response['Body'].iter_lines():
                doc = json.loads(line)

                # Total Items (including components of complex objects; some may not have associated files)
                doc_count = doc_count + 1

                print("\n********************************")
                print(f"{doc_count} {doc['path']}")

                extent = get_extent(doc)

                print(f"{extent}")

    print(f"{doc_count=}")

def get_extent(doc):
    extent = {
        "main_count": 0,
        "main_size": 0,
        "aux_count": 0,
        "aux_size": 0,
        "deriv_count": 0,
        "deriv_size": 0,
        "total_count": 0,
        "total_size": 0
    }

    properties = doc['properties']

    if properties.get('file:content', None):
        content = properties.get('file:content')
        extent['main_count'] = extent['main_count'] + 1
        extent['main_size'] = extent['main_size'] + int(content['length'])
        extent['total_count'] = extent['main_count'] + 1
        extent['total_size'] = extent['main_size'] + int(content['length'])
        print(f"main {extent['main_count']} {content['name']} {int(content['length'])}")

    # Original files vs file:content?
    if properties.get('picture:views', None):
        for view in properties.get('picture:views'):
            content = view['content']
            extent['deriv_count'] = extent['deriv_count'] + 1
            extent['deriv_size'] = extent['deriv_size'] + int(content['length'])
            extent['total_count'] = extent['total_count'] + 1
            extent['total_size'] = extent['total_size'] + int(content['length'])
            print(f"deriv {extent['deriv_count']} {content['name']} {view['description']} {int(content['length'])}")

    return extent




