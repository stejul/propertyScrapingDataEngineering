from dagster import solid, pipeline
import boto3

@solid(name="uploadObjectToS3", description="Upload given file to S3 server")
def upload_to_s3(): 

    s3 = boto3.client(service_name="s3", endpoint_url="http://localhost:9000", aws_access_key_id="admin", aws_secret_access_key="password")

    s3.create_bucket(Bucket="boban")

    s3.upload_file(Filename="src/data/apt_dump.csv", Bucket="boban", Key="testFile.csv")
    print("File has been uploaded to bucket")

@pipeline
def runIt():
    upload_to_s3()
