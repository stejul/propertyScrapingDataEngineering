from dagster import (
    Output,
    InputDefinition,
    OutputDefinition,
    solid,
    pipeline,
    ModeDefinition,
    List as DagsterList,
    String,
)
from dagster_aws.s3 import S3Coordinate
from typing import List
from os import walk
import boto3
import ntpath


@solid(
    name="uploadObjectToS3",
    input_defs=[
        InputDefinition(name="local_files", dagster_type=DagsterList[String]),
        InputDefinition(name="s3_coordinate", dagster_type=S3Coordinate),
    ],
    output_defs=[OutputDefinition(dagster_type=S3Coordinate)],
    description="Upload given file to S3 server",
)
def upload_to_s3(
    context, local_files: DagsterList[String], s3_coordinate: S3Coordinate
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
    output_defs=[OutputDefinition(dagster_type=DagsterList[String])],
)
def get_all_csv_files(context) -> DagsterList[String]:
    result: DagsterList[String] = []
    for (dirpath, dirname, filenames) in walk("src/data/"):
        for file in filenames:
            context.log.info(f"Found following file in directory: src/data/{file}")
            result.append(f"src/data/{file}")
    return result


@pipeline()
def execute_pipeline():
    upload_to_s3(local_files=get_all_csv_files())
