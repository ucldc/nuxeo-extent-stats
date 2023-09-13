import sys, os
import argparse
import extentreport
import boto3

def main(params):
    if params.datasource == 'es' and params.es_api_broken:
        query_db = True
    else:
        query_db = False

    campuses = [
        "UCB",
        "UCD",
        "UCI",
        "UCLA",
        "UCM",
        "UCOP",
        "UCR",
        "UCSB",
        "UCSC",
        "UCSD",
        "UCSF"
    ]

    if params.all:
        sub_prefixes = []
        for campus in campuses:
            sub_prefixes = get_child_prefixes(campus, params.datasource)
            workbook_id = f"{campus}-{params.datasource}"
            extentreport.report(workbook_id, campus, sub_prefixes, params.datasource, query_db)
    elif params.campus:
        sub_prefixes = get_child_prefixes(params.campus, params.datasource)
        workbook_id = f"{params.campus}-{params.datasource}"
        extentreport.report(workbook_id, params.campus, sub_prefixes, params.datasource, query_db)
    elif params.path:
        path = params.path.lstrip('/asset-library/')
        sub_prefixes = get_child_prefixes(path, params.datasource)
        campus_equiv = path.rstrip('/').replace('/', '_')
        workbook_id = f"{campus_equiv}-{params.datasource}"
        extentreport.report(workbook_id, campus_equiv, sub_prefixes, params.datasource, query_db)

def get_child_prefixes(folder, datasource):
    md_prefix = {
        "db": "metadata",
        "es": "metadata-es"
    }
    folder_prefix = f"{md_prefix[datasource]}/{folder.rstrip('/')}"
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=os.environ.get('S3_BUCKET'),
        Prefix=folder_prefix
    )

    prefixes = []
    folder_prefix_parts_count = len(folder_prefix.split('/'))
    for page in pages:
        for item in page['Contents']:
            parts = item['Key'].split('/')
            child_prefix = '/'.join(parts[0:folder_prefix_parts_count + 1])
            if not child_prefix in prefixes:
                prefixes.append(child_prefix)

    return prefixes

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help="create reports for all campuses", action="store_true")
    top_folder.add_argument('--campus', help="single campus")
    top_folder.add_argument('--path', help="nuxeo path for folder")
    parser.add_argument('--derivatives', help="include derivatives in file count", default=True)
    parser.add_argument('--datasource', choices=['es', 'db'], help="metadata source: es (elasticsearch) or db (database)", default='es')
    parser.add_argument('--es_api_broken', action="store_true", help="set this option when the Nuxeo elasticsearch API is broken")

    args = parser.parse_args()
    sys.exit(main(args))