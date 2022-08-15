import sys, os
import argparse
import extentreport
import boto3

BUCKET = os.environ.get('S3_BUCKET')
CAMPUSES = [
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
    "UCSF",
]

def main(params):

    if params.all:
        s3_prefixes = []
        for campus in CAMPUSES:
            s3_prefixes = get_child_prefixes(campus)
            extentreport.report(campus, s3_prefixes)
    elif params.campus:
        s3_prefixes = get_child_prefixes(params.campus)
        extentreport.report(params.campus, s3_prefixes)
    elif params.path:
        path = params.path.lstrip('/asset-library/')
        prefix = f"metadata/{path}"
        s3_prefixes = [prefix]
        workbook_id = path.rstrip('/').replace('/', '_')
        extentreport.report(workbook_id, s3_prefixes)

def get_child_prefixes(campus):
    campus_prefix = f"metadata/{campus}"
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=BUCKET,
        Prefix=campus_prefix
    )

    prefixes = []
    for page in pages:
        for item in page['Contents']:
            parts = item['Key'].split('/')
            child_prefix = '/'.join(parts[0:3])
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

    args = parser.parse_args()
    sys.exit(main(args))