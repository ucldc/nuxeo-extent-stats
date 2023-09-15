import json
import os
import requests
import sys
import urllib.parse

import boto3

import settings

''' fetch list of all folders in nuxeo at a given path (including the top folder itself) '''
def fetch_folder_list(path, outdir, depth=-1):
    uid = get_nuxeo_uid_for_path(path)

    folders = fetch_folders_recursive(uid, path, depth)

    if settings.DEBUG:
        fetchtolocal(outdir, folders)
    else:
        fetchtos3(outdir, folders)

    return folders

def fetch_folders_recursive(start_uid, path, depth=-1):
    folders = []

    def recurse(uid, depth):
        if depth != 0:
            response = run_folder_query(uid)
            records, count = get_records(response)
            if count > 10000:
                print(f"{response['resultsCount']} RESULTS FOR {path}")
            for record in records:
                folders.append(
                    {
                        'uid': record['uid'],
                        'path': record['path'],
                        'parent_uid': uid
                    }
                )
                recurse(record['uid'], depth - 1)

    recurse(start_uid, depth)

    return folders

def run_folder_query(uid):
    query = f"SELECT * FROM Organization " \
            f"WHERE ecm:parentId = '{uid}' " \
            f"AND ecm:isTrashed = 0"
    url = u'/'.join([settings.NUXEO_API, "search/lang/NXQL/execute"])
    headers = settings.NUXEO_REQUEST_HEADERS
    page_size = 1000
    params = {
        'pageSize': page_size,
        'currentPageIndex': 0,
        'query': query
    }
    request = {'url': url, 'headers': headers, 'params': params} 

    '''
    print(
        f"Fetching page"
        f" {request.get('params').get('currentPageIndex')} "
        f"at {request.get('url')} "
        f"with query {request.get('params').get('query')} "
        )
    '''

    response = requests.get(**request)
    response.raise_for_status()

    return response

def get_nuxeo_uid_for_path(path):
    ''' get nuxeo uid for doc at given path '''
    escaped_path = urllib.parse.quote(path, safe=' /')
    url = u'/'.join([settings.NUXEO_API, "path", escaped_path.strip('/')])
    headers = settings.NUXEO_REQUEST_HEADERS
    request = {'url': url, 'headers': headers}
    response = requests.get(**request)
    response.raise_for_status()
    json_response = response.json()

    return json_response['uid']

def get_records(http_resp):
    response = http_resp.json()

    count = response['resultsCount']

    documents = [{'uid': doc['uid'], 'path': doc['path']} for doc in response['entries']]

    return documents, count

def fetchtolocal(outdir, folders):
    outdir = f"{os.getcwd()}/output/folders/{outdir}"

    if not os.path.exists(outdir):
        os.makedirs(outdir)

    filename = os.path.join(outdir, "folders.json")
    f = open(filename, "w+")

    body = json.dumps(folders)

    print(f"Writing file://{filename}")
    f.write(body)
    f.write("\n")

def fetchtos3(prefix, folders):
    s3_client = boto3.client('s3')

    bucket = settings.S3_BUCKET
    body = json.dumps(folders)
    key = f"folders/{prefix}/folders.json"
    print(f"Writing s3://{bucket}/{key}")
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
        s3_client.put_object(
            ACL='bucket-owner-full-control',
            Bucket=bucket,
            Key=key,
            Body=body)
    except Exception as e:
        print(e)
