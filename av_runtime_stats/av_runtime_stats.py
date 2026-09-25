import json
import mimetypes
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

def main():
    '''
        Calculate total run-time of A/V files by Nuxeo campus
        Use `mediainfo` tool to get file metadata:
            - count of a/v objects
            - total runtime length (hours, mins)
            - average length
    '''
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    bucket = "rikolti-content"
    pages = paginator.paginate(
        Bucket=bucket,
        Prefix="media/22838/"
    )

    for page in pages:
        for item in page['Contents']:
            mimetype, encoding = mimetypes.guess_file_type(item['Key'])
            if mimetype.startswith(('video/', 'audio/')):
                duration = ""
                key = item['Key']
                signed_url = get_signed_url(s3_client, bucket, key)
                mediainfo = subprocess.check_output(["mediainfo", "--full", "--output=JSON", signed_url])
                mediainfo = json.loads(mediainfo)
                for track in mediainfo.get("media", {}).get("track", []):
                    if track.get("@type") == "Video":
                        duration = track.get("Duration")

if __name__ == "__main__":
    sys.exit(main())