import sys
import argparse
from datetime import datetime

import requests
import urllib.parse

import extentreport
import metadatafetcher
import settings

def get_nuxeo_uid_for_path(path):
    escaped_path = urllib.parse.quote(path, safe=' /')
    url = u'/'.join([settings.NUXEO_API, "path", escaped_path.strip('/')])
    headers = settings.NUXEO_REQUEST_HEADERS
    request = {'url': url, 'headers': headers}
    response = requests.get(**request)
    response.raise_for_status()
    json_response = response.json()
    uid = json_response['uid']

    return uid

def fetch_nuxeo_folders_recursive(start_uid, path, depth=-1):
    folders = []

    def recurse(uid, depth):
        if depth != 0:
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
            response = response.json()
            count = response['resultsCount']
            records = [{'uid': doc['uid'], 'path': doc['path']} for doc in response['entries']]

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

def main(params):
    if params.campus:
        campuses = [params.campus]
    elif params.all:
        campuses = settings.CAMPUSES

    for campus in campuses:
        print("**********************")
        print(f"******   {campus}   ******")
        print("**********************")

        if params.version:
            version = params.version
        else:
            version = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            uid = get_nuxeo_uid_for_path(f"/asset-library/{campus}")
            folders = fetch_nuxeo_folders_recursive(uid, 1)
            for folder in folders:
                next_page = {
                    "campus": campus,
                    "path": folder['path'],
                    "uid": folder['uid'],
                    "version": version
                }

                while next_page:
                    fetcher = metadatafetcher.Fetcher(next_page)
                    fetcher.fetch_page()
                    next_page = fetcher.next_page()

        extentreport.report(campus, version)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help="create reports for all campuses", action="store_true")
    top_folder.add_argument('--campus', help="single campus")
    parser.add_argument('--version', help="Metadata version. If not provided, metadata will be fetched from S3.")

    args = parser.parse_args()
    sys.exit(main(args))