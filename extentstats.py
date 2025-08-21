import sys, os
import argparse
from collections import namedtuple
from datetime import datetime
import json
import shutil

import boto3
import humanize
import requests
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import quote, urlparse
import xlsxwriter

CAMPUSES = os.environ.get('CAMPUSES')

METADATA = os.environ.get('NUXEO_EXTENT_STATS_METADATA')
REPORTS = os.environ.get('NUXEO_EXTENT_STATS_REPORTS')
TEMP = os.environ.get('NUXEO_EXTENT_STATS_LOCAL_TEMPDIR')

NUXEO_DBQUERY_URL = os.environ['NUXEO_DBQUERY_URL']
NUXEO_DBQUERY_TOKEN = os.environ['NUXEO_DBQUERY_TOKEN']

DataStorage = namedtuple(
    "DateStorage", "uri, store, bucket, path"
)

NUXEO_API_URL = os.environ.get('NUXEO_API_URL')
NUXEO_API_TOKEN = os.environ.get('NUXEO_API_TOKEN')

def parse_data_uri(data_uri: str):
    data_loc = urlparse(data_uri)
    return DataStorage(
        data_uri, data_loc.scheme, data_loc.netloc, data_loc.path)

DATA = parse_data_uri(METADATA)

def configure_http_session() -> requests.Session:
    http = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[413, 429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http

HTTP_SESSION = configure_http_session()

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


def get_nuxeo_uid_for_path(path):
    payload = {
        'path': quote(path, safe=' /'),
        'doc_type': 'records',
        'results_type': 'full',
        'relation': 'self'
    }
    response = requests.get(
        url=NUXEO_DBQUERY_URL,
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json'},
        cookies={'dbquerytoken': NUXEO_DBQUERY_TOKEN}
    )
    response.raise_for_status()

    return json.loads(response.text)['uid']

def fetch_folders(root):
    folders = []

    def recurse(pages):
        folders.extend(pages)
        for page in pages:
            child_folder_pages = get_pages_of_folders(page)
            recurse(child_folder_pages)

    # get root folders
    root_folder_pages = get_pages_of_folders(root)

    # recurse down the tree to fetch any nested folders
    recurse(root_folder_pages)

    return folders

def get_pages_of_folders(root_folder: dict):
    next_page = True
    resume_after = ''
    folders = []
    while next_page:
        resp = query_nuxeo_db_directly(root_folder, 'folders', 'full', resume_after)
        next_page = resp.json().get('isNextPageAvailable')
        resume_after = resp.json().get('resumeAfter')
        records = [{'uid': doc['uid'], 'path': doc['path']} for doc in resp.json().get('entries', [])]
        for record in records:
            folders.append(
                {
                    'uid': record['uid'],
                    'path': record['path'],
                    'parent_uid': root_folder['uid']
                }
            )

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
        for root, dirs, files in os.walk(metadata_dir):
            for file in files:
                filepath = os.path.join(root, file)
                with open(filepath, "r") as f:
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

    # Query database using Nuxeo API to get full metadata
    uid = json.loads(doc)['uid']
    full_metadata = hit_nuxeo_api(uid)
    doc_extent = get_extent(full_metadata)

    stats['docs'].append(f"{full_metadata['uid']}, {full_metadata['path']}\n")
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

def hit_nuxeo_api(uid):
    ''' Hit the Nuxeo API to get full record metadata '''
    url = u'/'.join([NUXEO_API_URL, "id", uid])
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-NXDocumentProperties": "*",
        "X-NXRepository": "default",
        "X-Authentication-Token": NUXEO_API_TOKEN
        }
    request = {'url': url, 'headers': headers}
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

def fetch_records(root: dict, campus: str, version: str):
    '''
        Fetch a listing of all records for a given root document
        in batches (pages) of 100, and write each page to storage.
    '''
    next_page = True
    resume_after = ''
    write_page = 0
    while next_page:
        resp = query_nuxeo_db_directly(root, 'records', 'listing', resume_after)
        next_page = resp.json().get('isNextPageAvailable')
        resume_after = resp.json().get('resumeAfter')
        records = resp.json().get('entries', [])

        if not records:
            next_page = False
            continue

        # write page of parent records to storage
        store_page_of_records(records, root['path'], campus, version, write_page)
        write_page += 1

        # get any component records and write to storage
        for record in records:
            fetch_components(record, campus, version, root)

def fetch_components(root_record: dict, campus: str, version: str, folder: dict):
    '''
    Fetch pages of components for a given record uid
    It is possible for components to be nested inside components; in the case
    of multiple layers, the hierarchy is ignored and all layers of components
    are considered to to be children of the root record.
    '''
    component_pages = []

    def recurse(pages):
        component_pages.extend(pages)
        for page in pages:
            records = page.get('entries', [])
            for record in records:
                child_component_pages = get_pages_of_child_components(record)
                recurse(child_component_pages)

    # get components of root record
    root_component_pages = get_pages_of_child_components(root_record)

    # recurse down the tree to fetch any nested components
    recurse(root_component_pages)

    # write component pages to storage
    page_count = 0
    for page in component_pages:
        records = page.get('entries', [])
        path = f"{folder['path']}/children"
        page_name = f"{root_record['uid']}-{page_count}"
        store_page_of_records(records, path, campus, version, page_name)
        page_count += 1


def get_pages_of_child_components(record: dict):
    next_page = True
    resume_after = ''
    components = []
    while next_page:
        resp = query_nuxeo_db_directly(record, 'records', 'listing', resume_after)
        next_page = resp.json().get('isNextPageAvailable')
        resume_after = resp.json().get('resumeAfter')
        records = resp.json().get('entries', [])

        if not records:
            next_page = False
            continue

        components.extend(records)

    # batch components into pages of 100
    # not sure if this is necessary, but let's copy the rikolti nuxeo fetcher
    # TODO: in python3.12 we can use itertools.batched
    batch_size = 100
    pages = []
    for i in range(0, len(components), batch_size):
        pages.append({"entries": components[i:i+batch_size]})

    return pages


def query_nuxeo_db_directly(root: dict, doc_type: str, results_type: str, resume_after: str):
    ''' Use the nuxeo cdl_dbquery lambda to fetch nuxeo records from the db '''
    payload = {
        'uid': root['uid'],
        'doc_type': doc_type,
        'results_type': results_type,
        'resume_after': resume_after
    }

    request = {
        'url': NUXEO_DBQUERY_URL,
        'headers': {'Content-Type': 'application/json'},
        'cookies': {'dbquerytoken': NUXEO_DBQUERY_TOKEN},
        'data': json.dumps(payload)
    }

    try:
        response = HTTP_SESSION.get(**request)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Unable to fetch page {request}")
        raise(e)
    return response


def store_page_of_records(records: list, path: str, campus: str, version: str, page_name: str):
    if DATA.store == 'file':
        folder_path = path.removeprefix(f'/asset-library/{campus}/')
        dir = os.path.join(DATA.path, campus, version, folder_path)
        filename = os.path.join(dir, f"{page_name}.jsonl")
        jsonl = "\n".join([json.dumps(record) for record in records])
        jsonl = f"{jsonl}\n"
        write_object_to_local(dir, filename, jsonl)
    elif DATA.store == 's3':
        folder_path = path.removeprefix(f'/asset-library/{campus}/')
        base_folder = DATA.path
        s3_key = f"{base_folder.lstrip('/')}/{campus}/{version}/{folder_path}/{page_name}.jsonl"
        jsonl = "\n".join([json.dumps(record) for record in records])
        load_object_to_s3(DATA.bucket, s3_key, jsonl)


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
            path = f"/asset-library/{campus}"
            uid = get_nuxeo_uid_for_path(path)
            for folder in fetch_folders({'uid': uid}):
                fetch_records(folder, campus, version)

        create_extent_report(campus, version)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help="create reports for all campuses", action="store_true")
    top_folder.add_argument('--campus', help="single campus")
    parser.add_argument('--version', help="Metadata version. If provided, metadata will be fetched from S3.")

    args = parser.parse_args()
    sys.exit(main(args))
