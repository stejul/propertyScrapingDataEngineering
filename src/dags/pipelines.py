from dagster import pipeline
from src.dags.upload_to_s3 import get_all_csv_files, upload_to_s3
from src.dags.scrapers import laendleimmo_crawlers


@pipeline(
    name="uploadDataDumpToS3",
    description="Upload objects given as a list of files to the S3 Storage Server",
)
def execute_s3_pipeline():
    # info = laendleimmo_info_collector(link_scraper=laendleimmo_link_collector())
    info = laendleimmo_crawlers()
    upload_to_s3(local_files=get_all_csv_files(info_scraper=info))
