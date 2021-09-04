from dagster import (
    Output,
    InputDefinition,
    OutputDefinition,
    solid,
    pipeline,
    List as DagsterList,
    String,
)
from dagster_aws.s3 import S3Coordinate
from os import walk
from dotenv import load_dotenv
import boto3
import ntpath
import os

load_dotenv()


@solid(
    name="uploadObjectToS3",
    description="""
    **Uploads the dump files to S3 Server**
    ### Authors
    stejul <https://github.com/stejul>
    """,
    input_defs=[
        InputDefinition(name="local_files", dagster_type=DagsterList[String]),
        InputDefinition(name="s3_coordinate", dagster_type=S3Coordinate),
    ],
    output_defs=[OutputDefinition(dagster_type=S3Coordinate)],
)
def upload_to_s3(
    context, local_files: DagsterList[String], s3_coordinate: S3Coordinate
) -> S3Coordinate:

    s3 = boto3.client(
        service_name="s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id=os.getenv("MINIO_USER"),
        aws_secret_access_key=os.getenv("MINIO_PASSWORD"),
    )

    s3Resource = boto3.resource(
        service_name="s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id=os.getenv("MINIO_USER"),
        aws_secret_access_key=os.getenv("MINIO_PASSWORD"),
    )

    return_s3_coordinate: S3Coordinate = {"bucket": s3_coordinate["bucket"]}
    for file in local_files:
        head, tail = ntpath.split(file)
        return_s3_coordinate["key"] = s3_coordinate["key"] + "/" + tail

        if s3Resource.Bucket(return_s3_coordinate["bucket"]).creation_date is None:
            s3.create_bucket(Bucket=return_s3_coordinate["bucket"])

        s3.upload_file(
            Filename=f"{head}/{tail}",
            Bucket=return_s3_coordinate["bucket"],
            Key=return_s3_coordinate["key"],
        )
        context.log.info(f"Uploaded successfully - {file}")

    return Output(return_s3_coordinate)


@solid(
    name="getListOfFiles",
    description="""
    Checks the data directory and returns a list of files
    """,
    output_defs=[OutputDefinition(dagster_type=DagsterList[String])],
)
def get_all_csv_files(context, info_scraper) -> DagsterList[String]:
    context.log.info(f"Info dump is available {info_scraper}")
    result: DagsterList[String] = []
    for (dirpath, dirname, filenames) in walk("src/data/"):
        for file in filenames:
            context.log.info(f"Found following file in directory: src/data/{file}")
            result.append(f"src/data/{file}")
    return result
