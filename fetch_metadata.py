import sys, os
import argparse
import json
import folderfetcher
import metadatafetcher
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
        for campus in CAMPUSES:
            folders = get_folder_list(campus, params.no_folder_refresh)
            fetch_metadata(folders, campus, params.datasource)
    elif params.campus:
        folders = get_folder_list(params.campus, params.no_folder_refresh)
        fetch_metadata(folders, params.campus, params.datasource)


def fetch_metadata(folders, campus, datasource):
    parent_uids = [folder['parent_uid'] for folder in folders]
    for folder in folders:
        if folder['uid'] in parent_uids:
            has_subfolder = True
        else:
            has_subfolder = False
        next_page = {
            "campus": {campus},
            "path": folder['path'],
            "uid": folder['uid'],
            "datasource": datasource,
            "has_subfolder": has_subfolder
        }

        while next_page:
            fetcher = metadatafetcher.Fetcher(next_page)
            fetcher.fetch_page()
            next_page = fetcher.next_page()


def get_folder_list(campus, no_refresh):
    if no_refresh:
        # get list of folders from s3
        folders = fetch_folder_list_from_s3(campus)
    else:
        # fetch list of folders and stash on s3
        campus_basepath = f"/asset-library/{campus}"
        # fetch folders to a depth of 4
        folders = folderfetcher.fetch(campus_basepath, campus, 4)

    return folders


def fetch_folder_list_from_s3(campus):
    key = f"folders/{campus}/folders.json"
    s3_client = boto3.client('s3')
    response = s3_client.get_object(
        Bucket=BUCKET,
        Key=key
    )

    return json.loads(response['Body'].read())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fetch metadata for nuxeo extent stats reporting")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help="create reports for all campuses", action="store_true")
    top_folder.add_argument('--campus', help="single campus")
    #top_folder.add_argument('--path', help="nuxeo path for folder")
    parser.add_argument('--datasource', choices=['es', 'db'], help="metadata source: es (elasticsearch) or db (database)", default='es')
    parser.add_argument('--no_folder_refresh', help="do not refresh list of folders for each campus", action="store_true")
    args = parser.parse_args()

    sys.exit(main(args))
