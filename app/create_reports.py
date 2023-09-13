import sys, os
import argparse

import boto3

import folderfetcher
import metadatafetcher
import extentreport

def fetch_metadata(campus, datasource):
    folders = folderfetcher.fetch(f"/asset-library/{campus}", campus, 1)
    parent_uids = [folder['parent_uid'] for folder in folders]
    for folder in folders:
        next_page = {
            "campus": campus,
            "path": folder['path'],
            "uid": folder['uid'],
            "datasource": datasource
        }

        while next_page:
            fetcher = metadatafetcher.Fetcher(next_page)
            fetcher.fetch_page()
            next_page = fetcher.next_page()


def main(params):
    if params.es_api_broken and params.datasource == 'es':
        query_db = True
    else:
        query_db = False

    if params.campus:
        campuses = [params.campus]
    elif params.all:
        campuses = os.environ.get('CAMPUSES')

    for campus in campuses:
        print("**********************")
        print(f"******   {campus}   ******")
        print("**********************")

        if not params.reportonly:
            fetch_metadata(campus, params.datasource)

        extentreport.report(campus, params.datasource, query_db)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help="create reports for all campuses", action="store_true")
    top_folder.add_argument('--campus', help="single campus")
    parser.add_argument('--derivatives', help="include derivatives in file count", default=True)
    parser.add_argument('--datasource', choices=['es', 'db'], help="metadata source: es (elasticsearch) or db (database)", default='es')
    parser.add_argument('--es_api_broken', action="store_true", help="set this option when the Nuxeo elasticsearch API is broken")
    parser.add_argument('--reportonly', action="store_true", help="set this option when the metadata already exists on S3 and does not need to be fetched")

    args = parser.parse_args()
    sys.exit(main(args))