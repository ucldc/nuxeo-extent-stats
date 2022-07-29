import sys, os
import boto3
import json
import xlsxwriter
import datetime
import humanize

BUCKET = os.environ.get('S3_BUCKET')

def report(path):
    path = path.lstrip('/asset-library/')
    prefix = f"metadata/{path}"

    doc_count = 0
    main_count = 0
    main_size = 0
    aux_count = 0
    aux_size = 0
    deriv_count = 0
    deriv_size = 0
    total_count = 0
    total_size = 0

    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix
    )

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

                #print("\n********************************")
                #print(f"{doc_count} {doc['path']}")

                extent = get_extent(doc)

                #print(f"{extent}")
                main_count = main_count + extent['main_count']
                main_size = main_size + extent['main_size']
                aux_count = aux_count + extent['aux_count']
                aux_size = aux_size + extent['aux_size']
                deriv_count = deriv_count + extent['deriv_count']
                deriv_size = deriv_size + extent['deriv_size']
                total_count = total_count + extent['total_count']
                total_size = total_size + extent['total_size']

                # TODO: deduplicate

    '''
    print(f"{doc_count=}")
    print(f"{main_count=}")
    print(f"{main_size=}")
    print(f"{aux_count=}")
    print(f"{aux_size=}")
    print(f"{deriv_count=}")
    print(f"{deriv_size=}")
    print(f"{total_count=}")
    print(f"{total_size=}")
    '''

    today = datetime.date.today().strftime('%Y-%m-%d')
    workbook = xlsxwriter.Workbook(f'{path}-extent-stats-{today}.xlsx')
    worksheet = workbook.add_worksheet('Summary')

    stats = [
        ["Doc Count", doc_count],
        ["Content File Count", main_count],
        ["Content File Size", humanize.naturalsize(main_size, binary=True)],
        ["Aux File Count", aux_count],
        ["Aux File Size", humanize.naturalsize(aux_size, binary=True)],
        ["Derivative File Count", deriv_count],
        ["Derivative File Size", humanize.naturalsize(deriv_size, binary=True)],
        ["Total File Count", total_count],
        ["Total File Size", humanize.naturalsize(total_size, binary=True)]
    ]

    bold = workbook.add_format({'bold': True})

    row = 0
    col = 0
    for stat in (stats):
        worksheet.write_string(0, col, f"{stat[0]}", bold)
        worksheet.write_string(1, col, f"{stat[1]}")
        col = col + 1

    workbook.close()



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
        extent['main_count'] = extent['main_count'] + 1
        extent['main_size'] = extent['main_size'] + int(content['length'])
        extent['total_count'] = extent['main_count'] + 1
        extent['total_size'] = extent['main_size'] + int(content['length'])
        #print(f"main {extent['main_count']} file:content {content['name']} {int(content['length'])}")

    # Original files vs file:content?
    if properties.get('picture:views'):
        for view in properties.get('picture:views'):
            content = view['content']
            extent['deriv_count'] = extent['deriv_count'] + 1
            extent['deriv_size'] = extent['deriv_size'] + int(content['length'])
            extent['total_count'] = extent['total_count'] + 1
            extent['total_size'] = extent['total_size'] + int(content['length'])
            #print(f"deriv {extent['deriv_count']} picture:views {content['name']} {view['description']} {int(content['length'])}")

    # extra_files:file
    if properties.get('extra_files:file'):
        file = properties.get('extra_files:file')
        for f in file:
            if f.get('blob'):
                blob = f.get('blob')
                extent['aux_count'] = extent['aux_count'] + 1
                extent['aux_size'] = extent['aux_size'] + int(blob['length'])
                extent['total_count'] = extent['total_count'] + 1
                extent['total_size'] = extent['total_size'] + int(blob['length'])
                #print(f"aux {extent['aux_count']} extra_files {blob['name']} {int(blob['length'])}")

    # files:files
    if properties.get('files:files'):
        files = properties.get('files:files')
        for file in files:
            if file.get('file'):
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
            if board.get('content'):
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
            if vid.get('content'):
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




