from minio import Minio
from dagster import solid

@solid
def upload_to_s3(context, url: str, bucket_name: str, blob): 
    client = Minio("s3.min.io", access_key="", secret_key="")

    found = client.bucket_exists(bucket_name)

    if not found:
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists")

    client.fput_object(bucket_name=bucket_name, object_name="testObj.csv", file_path=blob)
