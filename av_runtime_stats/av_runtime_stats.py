import argparse
import json
import subprocess
import sys

import boto3

def get_signed_url(s3_client, bucket, obj):
    """
    Generate a signed URL
    :param expires_in:  URL Expiration time in seconds
    :param bucket:
    :param obj:         S3 Key name
    :return:            Signed URL
    """
    presigned_url = s3_client.generate_presigned_url('get_object',
                    Params={'Bucket': bucket, 'Key': obj},
                    ExpiresIn=300)

    return presigned_url

def get_media_paths():
    """
    Parse json files containing media metadata from opensearch
    """

    """
    Query for parent metadata:

    GET /rikolti-prd/_search
    {
    "query": {
        "bool": {
        "must": [
            {"match": {"mapper_type": "nuxeo.nuxeo"}},
            {"terms": {"media.format": ["video", "audio"]}}
        ]
        }
    },
    "_source": [
        "collection_url",
        "campus_data",
        "media.path",
        "media.format",
        "media.mimtype"
    ],
    "size": 1200
    }

    Query for children metadata:

    GET /rikolti-prd/_search
    {
    "query": {
        "nested": {
        "path": "children",
        "query": {
            "terms": {
            "children.media.format": ["video", "audio"]
            }
        }
        }
    },
    "_source": [
        "collection_url",
        "campus_data",
        "children.media.path",
        "children.media.format",
        "children.media.mimtype"
    ],
    "size": 1700
    }
    """
    paths = []

    # get s3 paths for parent-level media
    parent_metadata_file = "opensearch_media.json"
    with open(parent_metadata_file) as f:
        parent_metadata = f.read()

    parent_metadata = json.loads(parent_metadata)
    for hit in parent_metadata.get("hits").get("hits"):
        path = hit.get("_source").get("media").get("path")
        paths.append(path)

    # get s3 paths for child-level media
    child_metadata_file = "opensearch_children_media.json"
    with open(child_metadata_file) as f:
        child_metadata = f.read()

    child_metadata = json.loads(child_metadata)
    for hit in child_metadata.get("hits").get("hits"):
        for child in hit.get("_source").get("children"):
            format = child.get("media").get("format")
            if format in ["audio", "video"]:
                paths.append(child.get("media").get("path"))

    return paths

def fetch_media_info():
    rikolti_content_bucket = "rikolti-content"
    mediainfo_bucket = "pad-nuxeo"
    s3_client = boto3.client('s3')
    for path in get_media_paths():
        rikolti_key = path.removeprefix("s3://rikolti-content/")
        signed_url = get_signed_url(s3_client, rikolti_content_bucket, rikolti_key)
        mediainfo = subprocess.check_output(["mediainfo", "--full", "--output=JSON", signed_url])

        mediainfo_key = f"av_runtime/mediainfo/{path.removeprefix('s3://rikolti-content/media/')}.json"
        print(f"Writing s3://{mediainfo_bucket}/{mediainfo_key}")
        try:
            s3_client.put_object(
                ACL='bucket-owner-full-control',
                Bucket=mediainfo_bucket,
                Key=mediainfo_key,
                Body=mediainfo)
        except Exception as e:
            print(f"ERROR loading to S3: {e}")

def extract_collection_info(metadata, campuses, collections):
    for hit in metadata.get("hits").get("hits"):
        source = hit.get("_source")
        collection_id = source.get("collection_url")[0]
        campus_data = source.get("campus_data")[0]
        campus_id = campus_data.split("::")[0]
        campus_name = campus_data.split("::")[1]

        if collection_id not in collections:
            collections[collection_id] = {"campus_id": campus_id}
        if campus_id not in campuses:
            campuses[campus_id] = {"campus_name": campus_name}

    return campuses, collections

def get_campuses():
    campuses = {}
    collections = {}

    # parse parent-level metadata
    parent_metadata_file = "opensearch_media.json"
    with open(parent_metadata_file) as f:
        parent_metadata = f.read()

    parent_metadata = json.loads(parent_metadata)
    campuses, collections = extract_collection_info(parent_metadata, campuses, collections)

    # parse child-level metadata
    child_metadata_file = "opensearch_children_media.json"
    with open(child_metadata_file) as f:
        child_metadata = f.read()

    child_metadata = json.loads(child_metadata)
    campuses, collections = extract_collection_info(child_metadata, campuses, collections)

    for campus_id in campuses:
        campus_collections = []
        for collection_id in collections:
            if collections[collection_id].get("campus_id") == campus_id:
                campus_collections.append(collection_id)
        campuses[campus_id]["collections"] = campus_collections

    return campuses

def humanize_duration(seconds):
    min, sec = divmod(seconds, 60)
    hour, min = divmod(min, 60)
    return '%dh%02dm%02ds' % (hour, min, sec)

def get_stats(campus):
    stats = {}
    no_duration = []
    collections = []
    campus_item_count = 0

    print(f"\n## {campus['campus_name']}")
    campus_duration = 0
    for collection_id in campus['collections']:
        # if collection_id not in ["40","28043"]:
        #     continue
        print(f"\n### Collection {collection_id}")

        collection_stats = {}
        collection_duration = 0
        collection_item_count = 0
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        bucket = "pad-nuxeo"
        pages = paginator.paginate(
            Bucket=bucket,
            Prefix=f"av_runtime/mediainfo/{collection_id}/"
        )

        for page in pages:
            for item in page['Contents']:
                item_duration = None
                key = item['Key']
                print(key)
                collection_item_count += 1
                campus_item_count += 1
                response = s3_client.get_object(
                    Bucket=bucket,
                    Key=key
                )
                mediainfo = response['Body'].read()
                mediainfo = json.loads(mediainfo)
                media = mediainfo.get("media", {})
                if media:
                    for track in mediainfo.get("media", {}).get("track", []):
                        if track.get("@type") == "General":
                            item_duration = float(track.get("Duration", 0))
                            if item_duration:
                                collection_duration += item_duration
                if not item_duration:
                    no_duration.append(key)

        collections.append(
                {
                    collection_id: {
                        "duration": collection_duration,
                        "duration_fomatted": humanize_duration(collection_duration),
                        "item_count": collection_item_count
                    }
                }
            )

        campus_duration += collection_duration

    stats["total_duration"] = campus_duration
    stats["total_duration_formatted"] = humanize_duration(campus_duration)
    stats["item_count"] = campus_item_count
    stats["collections"] = collections

    return stats, no_duration

def main(params):
    '''
        Calculate total run-time of A/V files by Nuxeo campus
    '''
    if params.fetch_mediainfo:
        print("Getting mediainfo")
        fetch_media_info()

    print("Assembling stats")
    stats = {}
    items_without_duration = {}
    campuses = get_campuses()
    for campus_id in campuses:
        campus = campuses[campus_id]
        campus_name = campus['campus_name']
        # if campus_name != "UC Berkeley":
        #     continue
        campus_stats, campus_items_no_duration = get_stats(campus)
        print("\n\n")
        print(json.dumps(campus_stats))
        
        stats[campus_name] = campus_stats
        items_without_duration[campus_name] = campus_items_no_duration

    # write output json to s3
    with open("stats.json", "w") as f:
        f.write(json.dumps(stats))

    with open("no_duration.json", "w") as f:
        f.write(json.dumps(items_without_duration))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="create nuxeo a/v runtime report")
    parser.add_argument('--fetch_mediainfo', help="fetch mediainfo", action="store_true")

    args = parser.parse_args()
    sys.exit(main(args))