import sys, os
import argparse
from collections import namedtuple
from datetime import datetime
import json
import math
import shutil

import boto3
import humanize
import requests
from urllib.parse import quote, urlparse
import xlsxwriter

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

DataStorage = namedtuple(
    "DateStorage", "uri, store, bucket, path"
)

def parse_data_uri(data_uri: str):
    data_loc = urlparse(data_uri)
    return DataStorage(
        data_uri, data_loc.scheme, data_loc.netloc, data_loc.path)


def load_file_to_s3(bucket, key, filepath):
    s3_client = boto3.client('s3')
    print(f"Writing s3://{bucket}/{key}")
    try:
        response = s3_client.upload_file(
            Filename=filepath,
            Bucket=bucket,
            Key=key
        )
    except Exception as e:
            print(f"ERROR loading to S3: {e}")


def load_object_to_s3(bucket, key, content):
    s3_client = boto3.client('s3')
    print(f"Writing s3://{bucket}/{key}")
    try:
        s3_client.put_object(
            ACL='bucket-owner-full-control',
            Bucket=bucket,
            Key=key,
            Body=content)
    except Exception as e:
        print(f"ERROR loading to S3: {e}")

def write_object_to_local(dir, filename, content):
    if not os.path.exists(dir):
        os.makedirs(dir)

    fullpath = os.path.join(dir, filename)
    print(f"Writing file://{fullpath}")
    with open(fullpath, "w") as f:
        f.write(content)

class Fetcher(object):
    def __init__(self, params):
        self.campus = params.get('campus')
        self.path = params.get('path')
        self.uid = params.get('uid')
        self.version = params.get('version')
        self.current_page_index = params.get('current_page_index', 0)
        self.write_page = params.get('write_page', 0)
        self.page_size = 100

    def fetch_page(self):
        page = self.build_fetch_request()
        response = requests.get(**page)
        response.raise_for_status()
        records = self.get_records(response)

        if len(records) > 0:
            data = parse_data_uri(METADATA)
            if data.store == 'file':
                folder_path = self.path.removeprefix(f'/asset-library/{self.campus}/')
                dir = os.path.join(data.path, self.campus, self.version, folder_path)
                filename = os.path.join(dir, f"{self.write_page}.jsonl")
                jsonl = "\n".join([json.dumps(record) for record in records])
                jsonl = f"{jsonl}\n"
                write_object_to_local(dir, filename, jsonl)
            elif data.store == 's3':
                folder_path = self.path.removeprefix(f'/asset-library/{self.campus}/')
                base_folder = data.path
                s3_key = f"{base_folder.lstrip('/')}/{self.campus}/{self.version}/{folder_path}/{self.write_page}.jsonl"
                jsonl = "\n".join([json.dumps(record) for record in records])
                load_object_to_s3(data.bucket, s3_key, jsonl)

        self.increment(response)

    def build_fetch_request(self):
        page = self.current_page_index
        if (page and page != -1) or page == 0:
            query = f"SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD " \
              f"WHERE ecm:ancestorId = '{self.uid}' AND " \
              f"ecm:isVersion = 0 AND " \
              f"ecm:isTrashed = 0 ORDER BY ecm:name"

            headers = NUXEO_REQUEST_HEADERS

            url = u'/'.join([NUXEO_API, "search/lang/NXQL/execute"])
            params = {
                'pageSize': f'{self.page_size}',
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
        self.page_count = math.ceil(results_count / self.page_size)
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

def get_nuxeo_uid_for_path(path):
    escaped_path = quote(path, safe=' /')
    url = u'/'.join([NUXEO_API, "path", escaped_path.strip('/')])
    headers = NUXEO_REQUEST_HEADERS
    request = {'url': url, 'headers': headers}
    response = requests.get(**request)
    response.raise_for_status()
    json_response = response.json()
    uid = json_response['uid']

    return uid

def get_campus_folders_from_nuxeo(start_uid, path, depth=-1):
    '''
    Get a list of Nuxeo folders below a given path, to a given depth
    Returns a list of dicts
    '''
    folders = []

    def recurse(uid, depth):
        if depth != 0:
            query = f"SELECT * FROM Organization " \
                    f"WHERE ecm:parentId = '{uid}' " \
                    f"AND ecm:isTrashed = 0"
            url = u'/'.join([NUXEO_API, "search/lang/NXQL/execute"])
            headers = NUXEO_REQUEST_HEADERS
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

def get_campus_folders_from_storage(campus, version):
    '''
    Metadata has aleady been fetched to storage (S3 or local);
    get a list of storage folders for this campus and version

    Returns a list of strings
    '''
    data = parse_data_uri(METADATA)

    if data.store == 'file':
        path = os.path.join(data.path, campus, version)
        folders = os.listdir(path)
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        bucket = data.bucket
        prefix = data.path
        prefix = prefix.lstrip('/')
        prefix = f"{prefix}/{campus}/{version}"
        pages = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix
        )

        folders = []
        folder_prefix_parts_count = len(prefix.split('/'))
        for page in pages:
            for item in page['Contents']:
                parts = item['Key'].split('/')
                child_prefix = '/'.join(parts[0:folder_prefix_parts_count + 1])
                if not child_prefix in folders:
                    folders.append(child_prefix)
    else:
        raise Exception(f"Unknown data scheme: {data.store}")

    return folders

MD5S = []
def create_extent_report(campus, version):
    '''
    for a given campus:
        - get metadata files for campus from S3
        - parse out file stats metadata
        - create spreadsheet of stats
    '''

    # create the excel excel_workbook
    tmp_dir = TEMP
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    excel_file_name = f"{campus}-extent-stats-{version}.xlsx"
    excel_file_path = os.path.join(tmp_dir, excel_file_name)
    excel_workbook = xlsxwriter.Workbook(excel_file_path)
    bold_format = excel_workbook.add_format({'bold': True})
    summary_worksheet = excel_workbook.add_worksheet('Summary')

    # write the headings
    headings = [
        "Project Folder",
        "Doc Count",
        "Unique Main File Count",
        "Main File Size",
        "Unique Files Tab Count",
        "Files Tab Count",
        "Unique Aux File Count",
        "Aux File Size",
        "Unique Derivative File Count",
        "Derivative File Size",
        "Total Unique File Count",
        "Total File Size"
    ]
    row = 0
    col = 0
    for h in (headings):
        summary_worksheet.write_string(0, col, h, bold_format)
        summary_worksheet.set_column(col, col, len(h))
        col = col + 1

    # increment
    row += 1

    # create a file to contain a list of all docs for QA purposes
    doclist_file_name = f"{campus}-doclist-{version}.txt"
    doclist_file_path = os.path.join(tmp_dir, doclist_file_name)
    if os.path.exists(doclist_file_path):
        os.remove(doclist_file_path)

    summary_doc_count = 0
    summary_stats = {
        "main_count": 0,
        "main_size": 0,
        "filetab_count": 0,
        "filetab_size": 0,
        "aux_count": 0,
        "aux_size": 0,
        "deriv_count": 0,
        "deriv_size": 0,
        "total_count": 0,
        "total_size": 0
    }

    folders = get_campus_folders_from_storage(campus, version)

    for folder in folders:
        print(f"Aggregating stats for {folder}")
        stats = get_stats(campus, version, folder)

        rowname = folder.split('/')[-1]
        write_stats(stats, summary_worksheet, row, rowname)
        row += 1

        if not os.path.exists(doclist_file_path):
            with open(doclist_file_path, "w") as f:
                for doc in stats['docs']:
                    f.write(f"{doc}")
        else:
            with open(doclist_file_path, "a") as f:
                for doc in stats['docs']:
                    f.write(f"{doc}")

        summary_doc_count += stats['doc_count']
        summary_stats['main_count'] += stats['main_count']
        summary_stats['main_size'] += stats['main_size']
        summary_stats['filetab_count'] += stats['filetab_count']
        summary_stats['filetab_size'] += stats['filetab_size']
        summary_stats['aux_count'] += stats['aux_count']
        summary_stats['aux_size'] += stats['aux_size']
        summary_stats['deriv_count'] += stats['deriv_count']
        summary_stats['deriv_size'] += stats['deriv_size']
        summary_stats['total_count'] += stats['total_count']
        summary_stats['total_size'] += stats['total_size']

    summary_stats['doc_count'] = summary_doc_count

    rowname = 'TOTALS'
    write_stats(summary_stats, summary_worksheet, row, rowname)

    excel_workbook.close()

    # write files to storage
    data = parse_data_uri(REPORTS)
    if data.store == 's3':
        key = f"{data.path}/{campus}/{version}/{excel_file_name}".lstrip('/')
        load_file_to_s3(data.bucket, key, excel_file_path)
        key = f"{data.path}/{campus}/{version}/{doclist_file_name}".lstrip('/')
        load_file_to_s3(data.bucket, key, doclist_file_path)
    elif data.store == 'file':
        dest_dir = os.path.join(data.path, campus, version)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        destination = os.path.join(dest_dir, excel_file_name)
        print(f"Writing file://{destination}")
        shutil.copyfile(excel_file_path, destination)

        destination = os.path.join(dest_dir, doclist_file_name)
        print(f"Writing file://{destination}")
        shutil.copyfile(doclist_file_path, destination)
    else:
        raise Exception(f"Unknown data scheme: {data.store}")

    # delete tmp files
    os.remove(excel_file_path)
    os.remove(doclist_file_path)


def get_stats(campus, version, folder):

    doc_count = 0

    stats = {
        "main_count": 0,
        "main_size": 0,
        "filetab_count": 0,
        "filetab_size": 0,
        "aux_count": 0,
        "aux_size": 0,
        "deriv_count": 0,
        "deriv_size": 0,
        "total_count": 0,
        "total_size": 0,
        "docs": []
    }

    data = parse_data_uri(METADATA)
    if data.store == 'file':
        metadata_dir = os.path.join(data.path, campus, version, folder)
        for file in os.listdir(metadata_dir):
            with open(os.path.join(metadata_dir, file), "r") as f:
                for line in f.readlines():
                    stats = add_doc_to_stats(stats, line)
                    doc_count += 1
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=data.bucket,
            Prefix=folder
        )
        for page in pages:
            for item in page['Contents']:
                #print(f"getting s3 object: {item['Key']}")
                response = s3_client.get_object(
                    Bucket=data.bucket,
                    Key=item['Key']
                )
                for line in response['Body'].iter_lines():
                    stats = add_doc_to_stats(stats, line)
                    doc_count += 1
    else:
        raise Exception(f"Unknown data scheme: {data.store}")

    # Total Items (including components of complex objects; some may not have associated files)
    stats['doc_count'] = doc_count

    return stats

def add_doc_to_stats(stats, doc):

    doc = json.loads(doc)
    doc_extent = get_extent(doc)

    stats['docs'].append(f"{doc['uid']}, {doc['path']}\n")
    stats['main_count'] += doc_extent['main_count']
    stats['main_size'] += doc_extent['main_size']
    stats['filetab_count'] += doc_extent['filetab_count']
    stats['filetab_size'] += doc_extent['filetab_size']
    stats['aux_count'] += doc_extent['aux_count']
    stats['aux_size'] += doc_extent['aux_size']
    stats['deriv_count'] += doc_extent['deriv_count']
    stats['deriv_size'] += doc_extent['deriv_size']
    stats['total_count'] += doc_extent['total_count']
    stats['total_size'] += doc_extent['total_size']

    return stats

def get_extent(doc):
    # query db for each record as a workaround while ES API endpoint is broken
    if NUXEO_API_ES_ENDPOINT_BROKEN:
        uid = doc['uid']
        doc = get_metadata_from_db(uid)

    extent = {
        "main_count": 0,
        "main_size": 0,
        "filetab_count": 0,
        "filetab_size": 0,
        "aux_count": 0,
        "aux_size": 0,
        "deriv_count": 0,
        "deriv_size": 0,
        "total_count": 0,
        "total_size": 0
    }

    properties = doc['properties']

    if properties.get('file:content'):
        content = properties.get('file:content')
        if not content['digest'] in MD5S:
            MD5S.append(content['digest'])
            extent['main_count'] += 1
            extent['main_size'] += int(content['length'])
            #print(f"main {extent['main_count']} file:content {content['name']} {int(content['length'])}")

    # Original files vs file:content?
    if properties.get('picture:views'):
        for view in properties.get('picture:views'):
            content = view['content']
            if not content['digest'] in MD5S:
                MD5S.append(content['digest'])
                extent['deriv_count'] += 1
                extent['deriv_size'] += int(content['length'])
                #print(f"deriv {extent['deriv_count']} picture:views {content['name']} {view['description']} {int(content['length'])}")

    # extra_files:file
    if properties.get('extra_files:file'):
        file = properties.get('extra_files:file')
        for f in file:
            if f.get('blob') and not f['blob']['digest'] in MD5S:
                blob = f.get('blob')
                MD5S.append(blob['digest'])
                extent['aux_count'] += 1
                extent['aux_size'] += int(blob['length'])
                #print(f"aux {extent['aux_count']} extra_files {blob['name']} {int(blob['length'])}")

    # files:files
    if properties.get('files:files'):
        files = properties.get('files:files')
        for file in files:
            if file.get('file') and not file['file']['digest'] in MD5S:
                file = file.get('file')
                extent['filetab_count'] += 1
                extent['filetab_size'] += int(file['length'])
                #print(f"filetab {extent['filetab_count']} files:files {file['name']} {int(file['length'])}")

    # vid:storyboard
    if properties.get('vid:storyboard'):
        storyboard = properties.get('vid:storyboard')
        for board in storyboard:
            if board.get('content') and not board['content']['digest'] in MD5S:
                content = board.get('content')
                extent['deriv_count'] += 1
                extent['deriv_size'] += int(content['length'])
                #print(f"deriv {extent['deriv_count']} storyboard {content['name']} {int(content['length'])}")

    # vid:transcodedVideos
    if properties.get('vid:transcodedVideos'):
        videos = properties.get('vid:transcodedVideos')
        for vid in videos:
            if vid.get('content') and not vid['content']['digest'] in MD5S:
                content = vid.get('content')
                extent['deriv_count'] += 1
                extent['deriv_size'] += int(content['length'])
                #print(f"deriv {extent['deriv_count']} vid:transcodedVideos {content['name']} {int(content['length'])}")

    # auxiliary_files:file
    if properties.get('auxiliary_files:file'):
        auxfiles = properties.get('auxiliary_files:file')
        for af in auxfiles:
            if af.get('content') and not af['content']['digest'] in MD5S:
                content = af.get('content')
                extent['deriv_count'] += 1
                extent['deriv_size'] += int(content['length'])

    # 3D
    if properties.get('threed:transmissionFormats'):
        formats = properties.get('threed:transmissionFormats')
        for format in formats:
            if format.get('content') and not format['content']['digest'] in MD5S:
                content = format.get('content')
                extent['deriv_count'] += 1
                extent['deriv_size'] += int(content['length'])

    extent['total_count'] = extent['main_count'] + extent['filetab_count'] + extent['deriv_count'] + extent['aux_count']
    extent['total_size'] = extent['main_size'] + extent['filetab_size'] + extent['deriv_size'] + extent['aux_size']

    return extent

def get_metadata_from_db(uid):
    url = u'/'.join([NUXEO_API, "id", uid])
    request = {'url': url, 'headers': NUXEO_REQUEST_HEADERS}
    response = requests.get(**request)
    response.raise_for_status()
    json_resp = response.json()
    return json_resp

def write_stats(stats, worksheet, rownum, rowname):

    formatted_data = [
        rowname,
        stats['doc_count'],
        stats['main_count'],
        humanize.naturalsize(stats['main_size'], binary=True),
        stats['filetab_count'],
        humanize.naturalsize(stats['filetab_size'], binary=True),
        stats['aux_count'],
        humanize.naturalsize(stats['aux_size'], binary=True),
        stats['deriv_count'],
        humanize.naturalsize(stats['deriv_size'], binary=True),
        stats['total_count'],
        humanize.naturalsize(stats['total_size'], binary=True)
    ]

    col = 0
    for d in (formatted_data):
        worksheet.write(rownum, col, d)
        col = col + 1

def main(params):
    if params.campus:
        campuses = [params.campus]
    elif params.all:
        campuses = CAMPUSES

    for campus in campuses:
        print("**********************")
        print(f"******   {campus}   ******")
        print("**********************")

        if params.version:
            version = params.version
        else:
            # fetch metadata from scratch from nuxeo
            version = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            uid = get_nuxeo_uid_for_path(f"/asset-library/{campus}")
            folders = get_campus_folders_from_nuxeo(uid, 1)
            for folder in folders:
                next_page = {
                    "campus": campus,
                    "path": folder['path'],
                    "uid": folder['uid'],
                    "version": version
                }

                while next_page:
                    fetcher = Fetcher(next_page)
                    fetcher.fetch_page()
                    next_page = fetcher.next_page()

        create_extent_report(campus, version)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help="create reports for all campuses", action="store_true")
    top_folder.add_argument('--campus', help="single campus")
    parser.add_argument('--version', help="Metadata version. If not provided, metadata will be fetched from S3.")

    args = parser.parse_args()
    sys.exit(main(args))