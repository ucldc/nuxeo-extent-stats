import os
import requests
from urllib.parse import urlparse
import json
import math

import settings

import boto3

PAGE_SIZE = 100

class Fetcher(object):
    def __init__(self, params):
        self.campus = params.get('campus')
        self.path = params.get('path')
        self.uid = params.get('uid')
        self.version = params.get('version')
        self.current_page_index = params.get('current_page_index', 0)
        self.write_page = params.get('write_page', 0)
        #self.md_prefix = "metadata"

    def fetch_page(self):
        page = self.build_fetch_request()
        response = requests.get(**page)
        response.raise_for_status()
        records = self.get_records(response)


        if len(records) > 0:
            data_loc = urlparse(settings.METADATA)
            if data_loc.scheme == 'file':
                self.fetchtolocal(records, data_loc.path)
            elif data_loc.scheme == 's3':
                self.fetchtos3(records, data_loc.netloc, data_loc.path)
            else:
                raise Exception(f"Unknown data scheme: {data_loc.scheme}")


        self.increment(response)

    def build_fetch_request(self):
        page = self.current_page_index
        if (page and page != -1) or page == 0:
            query = f"SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD " \
              f"WHERE ecm:ancestorId = '{self.uid}' AND " \
              f"ecm:isVersion = 0 AND " \
              f"ecm:isTrashed = 0 ORDER BY ecm:name"

            headers = settings.NUXEO_REQUEST_HEADERS

            url = u'/'.join([settings.NUXEO_API, "search/lang/NXQL/execute"])
            params = {
                'pageSize': f'{PAGE_SIZE}',
                'currentPageIndex': self.current_page_index,
                'query': query
            }

            request = {'url': url, 'headers': headers, 'params': params}
            '''
            print(
                f"Fetching page"
                f" {request.get('params').get('currentPageIndex')} "
                f"at {request.get('url')} "
                f"with query {request.get('params').get('query')} "
                f"for path {self.path}"
                )
            '''
        else:
            request = None
            print("No more pages to fetch")

        return request

    def get_records(self, http_resp):
        response = http_resp.json()

        results_count = response['resultsCount']
        self.page_count = math.ceil(results_count / PAGE_SIZE)
        if response['resultsCount'] > 10000:
            print(f"{response['resultsCount']} RESULTS FOR {self.path}")

        documents = [doc for doc in response['entries']]

        return documents

    def increment(self, http_resp):
        resp = http_resp.json()

        if resp.get('isNextPageAvailable'):
            self.current_page_index = self.current_page_index + 1
            self.write_page = self.write_page + 1
        # horrible hack to get around 10k limit using nuxeo search API endpoint
        elif self.current_page_index < self.page_count:
            self.current_page_index = self.current_page_index + 1
            self.write_page = self.write_page + 1
        else:
            self.current_page_index = -1

        return

    def fetchtolocal(self, records, base_path):
        folder_path = self.path.removeprefix(f'/asset-library/{self.campus}/')
        path = os.path.join(base_path, self.campus, self.version, folder_path)
        
        if not os.path.exists(path):
            os.makedirs(path)

        filename = os.path.join(path, f"{self.write_page}.jsonl")
        f = open(filename, "w+")

        jsonl = "\n".join([json.dumps(record) for record in records])
        print(f"Writing file://{filename}")
        f.write(jsonl)
        f.write("\n")

    def fetchtos3(self, records, bucket, base_path):
        s3_client = boto3.client('s3')
        folder_path = self.path.removeprefix(f'/asset-library/{self.campus}/')
        s3_key = f"{base_path.lstrip('/')}/{self.campus}/{self.version}/{folder_path}/{self.write_page}.jsonl"

        jsonl = "\n".join([json.dumps(record) for record in records])

        print(f"{bucket=}")
        print(f"{s3_key=}")
        print(f"Writing s3://{bucket}/{s3_key}")
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
            s3_client.put_object(
                ACL='bucket-owner-full-control',
                Bucket=bucket,
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
            "version": self.version,
            "current_page_index": self.current_page_index,
            "write_page": self.write_page
        }
