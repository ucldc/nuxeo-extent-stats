import sys, os
import boto3
import json
import xlsxwriter
from datetime import datetime
import pytz
import humanize
import requests

BUCKET = os.environ.get('S3_BUCKET')
MD5S = []

def report(workbook_id, campus, prefixes, datasource, query_db):
    '''
    given a list of s3 prefixes, create an xlsx spreadsheet
    containing extent stats for metadata contained in jsonl
    files with those prefixes
    '''

    # create the excel workbook
    #today = datetime.now(pytz.timezone('US/Pacific')).strftime('%Y%m%d-%H%M')
    today = datetime.now(pytz.timezone('US/Pacific')).strftime('%Y%m%d')
    outdir = os.path.join(os.getcwd(), "output", f"reports-{datasource}-{today}")
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    outfile = f"{workbook_id}-extent-stats-{today}.xlsx"
    outpath = os.path.join(outdir, outfile)
    workbook = xlsxwriter.Workbook(outpath)
    bold_format = workbook.add_format({'bold': True})
    summary_worksheet = workbook.add_worksheet('Summary')

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
    output_dir = os.path.join(os.getcwd(), 'output')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    doclist_dir = os.path.join(output_dir, f"doclists-{datasource}-{today}")
    if not os.path.exists(doclist_dir):
        os.mkdir(doclist_dir)

    doclist_file = f"{workbook_id}-doclist-{today}.txt"
    doclist_path = os.path.join(doclist_dir, doclist_file)
    if os.path.exists(doclist_path):
        os.remove(doclist_path)

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

    for prefix in prefixes:
        print(f"getting stats for {prefix}")
        stats = get_stats(prefix, doclist_path, query_db)

        rowname = prefix.split('/')[-1]
        write_stats(stats, summary_worksheet, row, rowname)
        row += 1

        # write doc info to a file
        if not os.path.exists(doclist_path):
            with open(doclist_path, "w") as f:
                for doc in stats['docs']:
                    f.write(f"{doc}")
        else:
            with open(doclist_path, "a") as f:
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

    workbook.close()

    # load files to S3
    report_prefixes = {
        "es": "reports-es",
        "db": "reports-db"
    }
    report_prefix = report_prefixes[datasource]
    load_to_s3(report_prefix, campus, outfile, outpath)
    load_to_s3(report_prefix, campus, doclist_file, doclist_path)

    # delete local files?

def get_stats(prefix, doclist_path, query_db):

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

    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix
    )

    for page in pages:

        for item in page['Contents']:
            #print(f"getting s3 object: {item['Key']}")
            response = s3_client.get_object(
                Bucket=BUCKET,
                Key=item['Key']
            )

            for line in response['Body'].iter_lines():
                doc = json.loads(line)

                # Total Items (including components of complex objects; some may not have associated files)
                doc_count += 1

                #print("\n********************************")
                #print(f"{doc_count} {doc['path']}")

                # query db for each record as a workaround while ES API endpoint is broken
                if query_db:
                    uid = doc['uid']
                    doc_md = get_metadata_from_db(uid)
                else:
                    doc_md = doc

                doc_extent = get_extent(doc_md)

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
                stats['docs'].append(f"{doc_md['uid']}, {doc_md['path']}\n")

    stats['doc_count'] = doc_count

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

def get_metadata_from_db(uid):
    # GET 'https://nuxeo.cdlib.org/nuxeo/site/api/v1/id/32f09ee0-dcc0-4746-90c4-ba4c710447cd' -H 'X-NXproperties: *' -H 'X-NXRepository: default' -H 'content-type: application/json' -u Administrator -p
    api_base = os.environ.get('NUXEO_API_BASE', 'https://nuxeo.cdlib.org/nuxeo/')
    api_path = os.environ.get('NUXEO_API_PATH', 'site/api/v1')
    url = u'/'.join([api_base, api_path, "id", uid])
    nuxeo_request_headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-NXDocumentProperties": "*",
                "X-NXRepository": "default",
                "X-Authentication-Token": os.environ.get('NUXEO_TOKEN')
                }
    request = {'url': url, 'headers': nuxeo_request_headers}
    response = requests.get(**request)
    response.raise_for_status()
    json_resp = response.json()
    return json_resp

def load_to_s3(report_prefix, campus, filename, filepath):
    s3_client = boto3.client('s3')
    s3_key = f"{report_prefix}/{campus}/{filename}"

    print(f"loading to s3 bucket {BUCKET} with key {s3_key}")
    try:
        response = s3_client.upload_file(
            Filename=filepath,
            Bucket=BUCKET,
            Key=s3_key
        )
    except Exception as e:
            print(f"ERROR loading to S3: {e}")

