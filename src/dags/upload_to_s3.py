from dagster import FileHandle, String, Output, OutputDefinition, solid, pipeline
from dagster_aws.s3 import S3Coordinate
from typing import List
from os import walk
import boto3
import os


@solid(
    name="uploadObjectToS3",
    output_defs=[OutputDefinition(dagster_type=S3Coordinate, name="s3Upload")],
    description="Upload given file to S3 server",
)
def upload_to_s3(
    context, local_files: List[str], s3_coordinate: S3Coordinate
) -> S3Coordinate:

    s3 = boto3.client(
        service_name="s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
    )

    s3Resource = boto3.resource(
        service_name="s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
    )

    for file in local_files:
        return_s3_coordinate: S3Coordinate = {
            "bucket": s3_coordinate["bucket"],
            "key": s3_coordinate["key"] + "/" + file,
        }

        if s3Resource.Bucket(return_s3_coordinate["bucket"]).creation_date is None:
            s3.create_bucket(Bucket=return_s3_coordinate["bucket"])

        context.log.info(file)
        s3.upload_file(
            Filename=file,
            Bucket=return_s3_coordinate["bucket"],
            Key=return_s3_coordinate["key"],
        )
        context.log.info("Uploaded successfully")
        return s3_coordinate
    return None


@solid(name="getListOfFiles")
def get_all_csv_files(context) -> str:
    for (dirpath, dirname, filenames) in walk("src/data/"):
        yield Output(f"src/data/{filenames}")


@pipeline
def execute_pipeline():
    upload_to_s3(local_files=get_all_csv_files())
