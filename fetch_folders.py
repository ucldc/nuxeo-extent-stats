import sys, os
import json
import folderfetcher

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

def main():
    for campus in campuses:

        # get folders for each campus
        campus_basepath = f"/asset-library/{campus}"

        # fetch folders to a depth of 1
        print(f"fetching folders for {campus_basepath}")
        folderfetcher.fetch(campus_basepath, campus, 1)

if __name__ == "__main__":
    sys.exit(main())
