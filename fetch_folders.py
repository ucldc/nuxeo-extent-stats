import sys, os
import json
from nuxeoextent import folderfetcher

DEBUG = os.environ.get('DEBUG', False)
if not DEBUG:
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

campuses = ["UCB"]

def main():
    for campus in campuses:

        # get folders for each campus
        campus_basepath = f"/asset-library/{campus}"

        # fetch folders to a depth of 3
        print(f"fetching folders for {campus_basepath}")
        folderfetcher.fetch(campus_basepath, campus, 3)

if __name__ == "__main__":
    sys.exit(main())
