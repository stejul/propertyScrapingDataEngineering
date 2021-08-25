from dagster import Any, String, Field, Bool, OutputDefinition, solid, pipeline
import boto3


@solid(
    name="uploadObjectToS3",
    description="Upload given file to S3 server",
    output_defs=[OutputDefinition(dagster_type=Bool, name="s3Client")],
    config_schema={
        "bucket_name": Field(
            String, is_required=True, description="Used name to create bucket"
        ),
        "filepath": Field(
            String,
            is_required=True,
            description="Where the filed is located on the disk",
        ),
        "s3_filepath": Field(
            String, is_required=True, description="How the file should be named on s3"
        ),
    },
)
def upload_to_s3(context, bucket_name: str, filepath: str, s3_filepath: str) -> Bool:

    s3 = boto3.client(
        service_name="s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
    )

    s3.create_bucket(Bucket=bucket_name)

    s3.upload_file(Filename=filepath, Bucket=bucket_name, Key=s3_filepath)
    context.log.info("Uploaded successfully")
    return True


@pipeline
def execute_pipeline():
    upload_to_s3()
