import sys, os
import json
import boto3
import argparse

'''
    Create extent stats report for content in Nuxeo
    based on prefetched metadata
'''

def main():
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--collection_id', help="registry collection ID")
    top_folder.add_argument('--campus', help="campus")
    parser.add_argument('--derivatives', default=False)
    # s3_bucket
    # outdir

    args = parser.parse_args()
    print(args)

    if args.collection_id:
        # get nuxeo path from registry
        collections = [args.collection_id]
    elif args.campus:
        # get all collection IDs for a campus
        # this means that everything needs to be set up in registry
        # or do we create one folder on s3 for each subfolder beneath /asset-library/UC*?

    


if __name__ == "__main__":
    sys.exit(main())