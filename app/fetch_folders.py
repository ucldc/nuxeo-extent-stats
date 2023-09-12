import sys, os
import json
import folderfetcher
import argparse

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

def main(params):
    if params.all:
        for campus in campuses:
            fetch_folders(campus, params.depth)
    elif params.campus:
        fetch_folders(params.campus, params.depth)

def fetch_folders(campus, depth):
    campus_basepath = f"/asset-library/{campus}"

    # fetch folders to a depth of 1
    print(f"fetching folders for {campus_basepath}")
    folderfetcher.fetch(campus_basepath, campus, depth)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help="create reports for all campuses", action="store_true")
    top_folder.add_argument('--campus', help="single campus")
    parser.add_argument('--depth', help='max depth for folders, i.e. /asset-library/UCX/sub1/sub2', type=int, default=1)
    args = parser.parse_args()

    sys.exit(main(args))
