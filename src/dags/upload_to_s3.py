from dagster import (
    Any,
    String,
    Field,
    Bool,
    LocalFileHandle,
    OutputDefinition,
    solid,
    pipeline,
)
from dagster_aws.s3 import S3Coordinate
import boto3
import os


@solid(name="uploadObjectToS3", description="Upload given file to S3 server")
def upload_to_s3(
    context, local_file: LocalFileHandle, s3_coordinate: S3Coordinate
) -> S3Coordinate:

    return_s3_coordinate: S3Coordinate = {
        "bucket": s3_coordinat["bucket"],
        "key": s3_coordiante["key"] + "/" + os.path.basename(local_file.path),
    }
    s3 = boto3.client(
        service_name="s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
    )

    s3.create_bucket(Bucket=return_s3_coordinate["bucket"])

    s3.upload_file(
        Filename=local_file,
        Bucket=return_s3_coordinate["bucket"],
        Key=return_s3_coordinate["key"],
    )
    context.log.info("Uploaded successfully")
    return s3_coordinate


@pipeline
def execute_pipeline():
    upload_to_s3()
