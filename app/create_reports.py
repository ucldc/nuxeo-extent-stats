import sys, os
import argparse

import boto3

import extentreport
import folderfetcher
import metadatafetcher
import settings

def main(params):
    if params.campus:
        campuses = [params.campus]
    elif params.all:
        campuses = setings.CAMPUSES

    for campus in campuses:
        print("**********************")
        print(f"******   {campus}   ******")
        print("**********************")

        if not params.reportonly:
            folders = folderfetcher.fetch_folder_list(f"/asset-library/{campus}", campus, 1)
            for folder in folders:
                next_page = {
                    "campus": campus,
                    "path": folder['path'],
                    "uid": folder['uid']
                }

                while next_page:
                    fetcher = metadatafetcher.Fetcher(next_page)
                    fetcher.fetch_page()
                    next_page = fetcher.next_page()

        extentreport.report(campus)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help="create reports for all campuses", action="store_true")
    top_folder.add_argument('--campus', help="single campus")
    parser.add_argument('--reportonly', action="store_true", help="metadata already exists on S3 and does not need to be fetched")

    args = parser.parse_args()
    sys.exit(main(args))