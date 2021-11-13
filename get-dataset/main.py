import os
import wget
import time
from google.cloud import storage

timestr = time.strftime("%Y%m%d-%H%M%S")
tmp_path = '/tmp/' + timestr
url = os.environ['URL']
bucket_name = os.environ['BUCKET']


def download(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    wget.download(url, tmp_path)

    obj = storage.Blob(timestr, bucket)
    obj.upload_from_filename(tmp_path)
