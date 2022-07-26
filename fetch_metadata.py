import sys, os
import argparse
import json
from nuxeoextent import folderfetcher
from lambda_function import lambda_handler
import boto3

BUCKET = os.environ.get('S3_BUCKET')


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
    "UCSF",
]

campuses = ['UCB']

def main(no_folder_refresh=False):

    for campus in campuses:
        if no_folder_refresh:
            # get list of folders from s3
            folders = get_folder_list(campus)
        else:
            # fetch list of folders and stash on s3
            campus_basepath = f"/asset-library/{campus}"
            # fetch folders to a depth of 3
            folders = folderfetcher.fetch(campus_basepath, campus, 4)
        
        for folder in folders:
            # fetch metadata
            #print(f"{folder=}")
            # launch metadatafetcher in lambda
            payload = {
                "campus": f"{campus}",
                "path": f"{folder['path']}",
                "uid": f"{folder['uid']}"
            }
            
            lambda_handler(json.dumps(payload), {})

def get_folder_list(campus):
    key = f"{campus}/folders.json"
    s3_client = boto3.client('s3')
    response = s3_client.get_object(
        Bucket=BUCKET,
        Key=key
    )

    return json.loads(response['Body'].read()) 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fetch metadata for nuxeo extent stats reporting")
    parser.add_argument('--no_folder_refresh', help="do not refresh list of folders for each campus", action="store_true")
    args = parser.parse_args()

    sys.exit(main(args.no_folder_refresh))
