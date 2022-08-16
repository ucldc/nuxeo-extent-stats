import os
import requests
import urllib.parse
import json

DEBUG = os.environ.get('DEBUG', False)
if DEBUG is False:
    import boto3

NUXEO_TOKEN = os.environ.get('NUXEO_TOKEN')
API_BASE = os.environ.get('NUXEO_API_BASE', 'https://nuxeo.cdlib.org/nuxeo/')
API_PATH = os.environ.get('NUXEO_API_PATH', 'site/api/v1')
BUCKET = os.environ.get('S3_BUCKET')

CHILD_NXQL = "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD " \
              "WHERE ecm:parentId = '{}' AND " \
              "ecm:isVersion = 0 AND " \
              "ecm:isTrashed = 0 ORDER BY ecm:name" \

ANCESTOR_NXQL = "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD " \
              "WHERE ecm:ancestorId = '{}' AND " \
              "ecm:isVersion = 0 AND " \
              "ecm:isTrashed = 0 ORDER BY ecm:name" \

NUXEO_REQUEST_HEADERS = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-NXDocumentProperties": "*",
                "X-NXRepository": "default",
                "X-Authentication-Token": NUXEO_TOKEN
                }

class Fetcher(object):
    def __init__(self, params):
        self.campus = params.get('campus')
        self.path = params.get('path')
        self.uid = params.get('uid', None)
        if self.uid is None:
            self.uid = self.get_nuxeo_uid_for_path(self.path)
        self.current_page_index = params.get('current_page_index', 0)
        self.write_page = params.get('write_page', 0)    

    def fetch_page(self):
        page = self.build_fetch_request()
        response = requests.get(**page)
        response.raise_for_status()
        records = self.get_records(response)

        if len(records) > 0:
            if DEBUG is True:
                self.fetchtolocal(records)
            else:
                self.fetchtos3(records)

        self.increment(response)

    def build_fetch_request(self):
        page = self.current_page_index
        if (page and page != -1) or page == 0:
            if self.path.count('/') == 6:
                # what about if there are more than 10k ancestors?
                # e.g. /asset-library/UCM/UCCE/Tulare - component objects
                # have to go one level deeper
                query = ANCESTOR_NXQL.format(self.uid)
            else:
                query = CHILD_NXQL.format(self.uid)
            url = u'/'.join([API_BASE, API_PATH, "search/lang/NXQL/execute"])
            headers = NUXEO_REQUEST_HEADERS
            params = {
                'pageSize': '100',
                'currentPageIndex': self.current_page_index,
                'query': query
            }
            request = {'url': url, 'headers': headers, 'params': params}
            print(
                f"Fetching page"
                f" {request.get('params').get('currentPageIndex')} "
                #f"at {request.get('url')} "
                #f"with query {request.get('params').get('query')} "
                f"for path {self.path}"
                )
        else:
            request = None
            print("No more pages to fetch")

        return request

    def get_nuxeo_uid_for_path(self, path):
        ''' get nuxeo uid for doc at given path '''
        escaped_path = urllib.parse.quote(path, safe=' /')
        url = u'/'.join([API_BASE, API_PATH, "path", escaped_path.strip('/')])
        headers = NUXEO_REQUEST_HEADERS
        request = {'url': url, 'headers': headers}
        response = requests.get(**request)
        response.raise_for_status()
        json_response = response.json()

        return json_response['uid']

    def get_records(self, http_resp):
        response = http_resp.json()

        if response['resultsCount'] > 10000:
            print(f"{response['resultsCount']} RESULTS FOR {self.path}")

        documents = [doc for doc in response['entries']]

        return documents

    def increment(self, http_resp):
        resp = http_resp.json()

        if resp.get('isNextPageAvailable'):
            self.current_page_index = self.current_page_index + 1
            self.write_page = self.write_page + 1
        else:
            self.current_page_index = -1

        return

    def fetchtolocal(self, records):
        nuxeo_path = self.path.lstrip(f'/asset-library/{self.campus}')
        nuxeo_path = nuxeo_path.strip()
        path = f"{os.getcwd()}/{self.campus}/{nuxeo_path}"
        path = f"{os.getcwd()}/metadata/{self.campus}/{nuxeo_path}"
        
        if not os.path.exists(path):
            os.makedirs(path)

        filename = os.path.join(path, f"{self.write_page}.jsonl")
        f = open(filename, "w+")

        jsonl = "\n".join([json.dumps(record) for record in records])
        print(f"writing to {filename}")
        f.write(jsonl)
        f.write("\n")

    def fetchtos3(self, records):
        s3_client = boto3.client('s3')
        folder_path = self.path.lstrip('/asset-library/')
        s3_key = f"metadata/{folder_path}/{self.write_page}.jsonl"

        jsonl = "\n".join([json.dumps(record) for record in records])

        print(f"loading to s3 bucket {BUCKET} with key {s3_key}")
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
            s3_client.put_object(
                ACL='bucket-owner-full-control',
                Bucket=BUCKET,
                Key=s3_key,
                Body=jsonl)
        except Exception as e:
            print(f"ERROR loading to S3: {e}")

    def next_page(self):
        if self.current_page_index == -1:
            return None

        return {
            "campus": self.campus,
            "path": self.path,
            "uid": self.uid,
            "current_page_index": self.current_page_index,
            "write_page": self.write_page
        }
