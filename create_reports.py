import sys
import argparse
import extentreport

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
        paths = [f"/asset-library/{campus}" for campus in CAMPUSES]
    elif params.path:
        paths = [params.path]
    elif params.campus:
        paths = [f"/asset-library/{params.campus}"]

    for path in paths:
        extentreport.report(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo extent stats report(s)")
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help="create reports for all campuses", action="store_true")
    top_folder.add_argument('--path', help="nuxeo path for folder")
    top_folder.add_argument('--campus', help="campus")
    parser.add_argument('--derivatives', help="include derivatives in file count", default=True)

    args = parser.parse_args()
    sys.exit(main(args))