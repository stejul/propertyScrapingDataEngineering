from dagster import (
    Output,
    InputDefinition,
    OutputDefinition,
    solid,
    pipeline,
    List as DagsterList,
    String,
)
from scrapy.crawler import CrawlerRunner
from src.scraper.apt_data_scraper import ImmoAptInfoScraper
from src.scraper.apt_list_scraper import ImmoAptListScraper
from twisted.internet import reactor, defer

crawler_process = CrawlerRunner()


@defer.inlineCallbacks
def crawl():
    yield crawler_process.crawl(ImmoAptListScraper)
    yield crawler_process.crawl(ImmoAptInfoScraper)
    reactor.stop()


@solid(
    name="laendleImmoCrawlers",
    description="""
    Run all *laendleimmo* crawlers

    ### Crawlers
    -   Collect all apartments and save their links into csv
    -   Crawl the links and extract information
        -   Price
        -   District
        -   Size
        -   Address
    ### Authors
    stejul <https://github.com/stejul>
    """,
)
def laendleimmo_crawlers(context):
    context.log.info("running crawlers")
    crawl()
    reactor.run()
