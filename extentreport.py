import sys, os
import boto3
import json
import xlsxwriter
import datetime
import humanize

BUCKET = os.environ.get('S3_BUCKET')
MD5S = []

today = datetime.date.today().strftime('%Y-%m-%d')
workbook = xlsxwriter.Workbook(f'TEST-extent-stats-{today}.xlsx')
bold = workbook.add_format({'bold': True})
summary_worksheet = workbook.add_worksheet('Summary')

def report(prefixes):
    '''
    given a list of s3 prefixes, create an xlsx spreadsheet
    containing extent stats for metadata contained in jsonl
    files with those prefixes

    first sheet will be a summary of all data

    subsequent sheets will list stats for each prefix

    '''
    summary_doc_count = 0
    summary_stats = {
        "main_count": 0,
        "main_size": 0,
        "aux_count": 0,
        "aux_size": 0,
        "deriv_count": 0,
        "deriv_size": 0,
        "total_count": 0,
        "total_size": 0
    }

    for prefix in prefixes:
        print(f"getting stats for {prefix}")
        stats = get_stats(prefix)

        sheetname = prefix.split('/')[-1]
        worksheet = workbook.add_worksheet(sheetname)
        write_stats(stats, sheetname)

        summary_doc_count += stats['doc_count']
        summary_stats['main_count'] += stats['main_count']
        summary_stats['main_size'] += stats['main_size']
        summary_stats['aux_count'] += stats['aux_count']
        summary_stats['aux_size'] += stats['aux_size']
        summary_stats['deriv_count'] += stats['deriv_count']
        summary_stats['deriv_size'] += stats['deriv_size']
        summary_stats['total_count'] += stats['total_count']
        summary_stats['total_size'] += stats['total_size']

    summary_stats['doc_count'] = summary_doc_count

    write_stats(summary_stats, summary_worksheet.name)

    workbook.close()

def get_stats(prefix):

    doc_count = 0

    stats = {
        "main_count": 0,
        "main_size": 0,
        "aux_count": 0,
        "aux_size": 0,
        "deriv_count": 0,
        "deriv_size": 0,
        "total_count": 0,
        "total_size": 0
    }

    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix
    )

    for page in pages:

        for item in page['Contents']:
            print(f"getting {item['Key']}")
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

                doc_extent = get_extent(doc)

                stats['main_count'] += doc_extent['main_count']
                stats['main_size'] += doc_extent['main_size']
                stats['aux_count'] += doc_extent['aux_count']
                stats['aux_size'] += doc_extent['aux_size']
                stats['deriv_count'] += doc_extent['deriv_count']
                stats['deriv_size'] += doc_extent['deriv_size']
                stats['total_count'] += doc_extent['total_count']
                stats['total_size'] += doc_extent['total_size']

    stats['doc_count'] = doc_count

    return stats

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

    if properties.get('file:content'):
        content = properties.get('file:content')
        if not content['digest'] in MD5S:
            MD5S.append(content['digest'])
            extent['main_count'] = extent['main_count'] + 1
            extent['main_size'] = extent['main_size'] + int(content['length'])
            extent['total_count'] = extent['main_count'] + 1
            extent['total_size'] = extent['main_size'] + int(content['length'])
            #print(f"main {extent['main_count']} file:content {content['name']} {int(content['length'])}")

    # Original files vs file:content?
    if properties.get('picture:views'):
        for view in properties.get('picture:views'):
            content = view['content']
            if not content['digest'] in MD5S:
                MD5S.append(content['digest'])
                extent['deriv_count'] = extent['deriv_count'] + 1
                extent['deriv_size'] = extent['deriv_size'] + int(content['length'])
                extent['total_count'] = extent['total_count'] + 1
                extent['total_size'] = extent['total_size'] + int(content['length'])
                #print(f"deriv {extent['deriv_count']} picture:views {content['name']} {view['description']} {int(content['length'])}")

    # extra_files:file
    if properties.get('extra_files:file'):
        file = properties.get('extra_files:file')
        for f in file:
            if f.get('blob') and not f['blob']['digest'] in MD5S:
                blob = f.get('blob')
                MD5S.append(blob['digest'])
                extent['aux_count'] = extent['aux_count'] + 1
                extent['aux_size'] = extent['aux_size'] + int(blob['length'])
                extent['total_count'] = extent['total_count'] + 1
                extent['total_size'] = extent['total_size'] + int(blob['length'])
                #print(f"aux {extent['aux_count']} extra_files {blob['name']} {int(blob['length'])}")

    # files:files
    if properties.get('files:files'):
        files = properties.get('files:files')
        for file in files:
            if file.get('file') and not file['file']['digest'] in MD5S:
                file = file.get('file')
                extent['main_count'] = extent['main_count'] + 1
                extent['main_size'] = extent['main_size'] + int(file['length'])
                extent['total_count'] = extent['main_count'] + 1
                extent['total_size'] = extent['main_size'] + int(file['length'])
                #print(f"main {extent['main_count']} files:files {file['name']} {int(file['length'])}")

    # vid:storyboard
    if properties.get('vid:storyboard'):
        storyboard = properties.get('vid:storyboard')
        for board in storyboard:
            if board.get('content') and not board['content']['digest'] in MD5S:
                content = board.get('content')
                extent['deriv_count'] = extent['deriv_count'] + 1
                extent['deriv_size'] = extent['deriv_size'] + int(content['length'])
                extent['total_count'] = extent['total_count'] + 1
                extent['total_size'] = extent['total_size'] + int(content['length'])
                #print(f"deriv {extent['deriv_count']} storyboard {content['name']} {int(content['length'])}")

    # vid:transcodedVideos
    if properties.get('vid:transcodedVideos'):
        videos = properties.get('vid:transcodedVideos')
        for vid in videos:
            if vid.get('content') and not vid['content']['digest'] in MD5S:
                content = vid.get('content')
                extent['deriv_count'] = extent['deriv_count'] + 1
                extent['deriv_size'] = extent['deriv_size'] + int(content['length'])
                extent['total_count'] = extent['total_count'] + 1
                extent['total_size'] = extent['total_size'] + int(content['length'])
                #print(f"deriv {extent['deriv_count']} vid:transcodedVideos {content['name']} {int(content['length'])}")

    # auxiliary_files:file
    if properties.get('auxiliary_files:file'):
        auxfiles = properties.get('auxiliary_files:file')
        print(f"auxfiles {auxfiles}")
        # TODO

    # 3D
    if properties.get('threed:transmissionFormats'):
        threed = properties.get('threed:transmissionFormats')
        print(f"auxfiles {threed}")
        # TODO

    return extent

def write_stats(stats, sheetname):
    excel_formatted_data = [
        ["Doc Count", stats['doc_count']],
        ["Unique Main File Count", stats['main_count']],
        ["Main File Size", humanize.naturalsize(stats['main_size'], binary=True)],
        ["Unique Aux File Count", stats['aux_count']],
        ["Aux File Size", humanize.naturalsize(stats['aux_size'], binary=True)],
        ["Unique Derivative File Count", stats['deriv_count']],
        ["Derivative File Size", humanize.naturalsize(stats['deriv_size'], binary=True)],
        ["Total Unique File Count", stats['total_count']],
        ["Total File Size", humanize.naturalsize(stats['total_size'], binary=True)]
    ]

    worksheet = workbook.get_worksheet_by_name(f"{sheetname}")

    row = 0
    col = 0
    for d in (excel_formatted_data):
        worksheet.write_string(0, col, f"{d[0]}", bold)
        worksheet.write_string(1, col, f"{d[1]}")
        col = col + 1



