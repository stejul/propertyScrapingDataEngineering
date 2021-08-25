import scrapy
import pandas as pd


class ImmoAptInfoScraper(scrapy.Spider):
    name = "immolaendle_apt_info_scraper"

    # start_urls = ["https://www.laendleimmo.at/immobilien/wohnung/terrassenwohnung/vorarlberg/feldkirch/150653?searchPageNumer=1"]

    custom_settings = {
        "AUTOTHROTTLE_ENABLED": True,
        "FEED_EXPORT_ENCODING": "utf-8-sig",
        "DOWNLOADER_MIDDLEWARE": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "scrapy_useragents.downloadermiddlewares.useragents.UserAgentsMiddleware": 500,
        },
        "USER_AGENTS": [
            (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/57.0.2987.110 "
                "Safari/537.36"
            ),  # chrome
            (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/61.0.3163.79 "
                "Safari/537.36"
            ),  # chrome
            (
                "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) "
                "Gecko/20100101 "
                "Firefox/55.0"
            ),  # firefox
        ],
    }

    link_list = {
        "title": [],
        "price": [],
        "number_of_rooms": [],
        "apt_size": [],
        "district": [],
        "city": [],
        "street_address": [],
    }

    def start_requests(self):
        df = pd.read_csv("src/data/apt_dump.csv")

        urlList = df["link"].to_list()

        for item in urlList:
            yield scrapy.Request(url=item, callback=self.parse)

    def parse(self, response):
        for item in response.css("body div.take-left"):
            # self.link_list["title"].append(item.css("div.row-eq-height div.col-md-9 h1.light::text").get())
            # self.link_list["price"].append(item.css("div.row-eq-height div.price-right div.detail-price::text").get())
            # self.link_list["number_of_rooms"].append(item.css("div.facts-set div.rooms-number div.facts-text div.sub-text::text").get())
            # self.link_list["apt_size"].append(item.css("div.facts-set div.house-surface div.facts-text div.sub-text::text").get())
            # self.link_list["district"].append(item.css("div.facts-m-space div.row div.col-md-12 p.fs18 a::text").getall()[1])
            # self.link_list["city"].append(item.css("div.facts-m-space div.row div.col-md-12 p.fs18 a::text").getall()[2])
            # self.link_list["street_address"].append(item.css("div.facts-m-space div.row div.col-md-12 p.fs18::text").getall()[2])

            self.link_list["title"].append(
                item.xpath(
                    "//body/div[1]/div[11]/div/div[1]/div[2]/div[2]/div[1]/h1/text()"
                ).get()
            )
            self.link_list["price"].append(
                item.xpath(
                    "//body/div[1]/div[11]/div/div[1]/div[2]/div[2]/div[2]/div[1]/text()"
                ).get()
            )
            self.link_list["number_of_rooms"].append(
                item.xpath(
                    "//body/div[1]/div[11]/div/div[2]/div/div[1]/div/div[2]/div[2]/div/text()"
                ).get()
            )
            self.link_list["apt_size"].append(
                item.xpath(
                    "//body/div[1]/div[11]/div/div[2]/div/div[1]/div/div[3]/div[2]/div/text()"
                ).get()
            )
            self.link_list["district"].append(
                item.xpath(
                    "//body/div[1]/div[11]/div/div[1]/div[1]/p/a[2]/text()"
                ).get()
            )
            self.link_list["city"].append(
                item.xpath(
                    "//body/div[1]/div[11]/div/div[1]/div[1]/p/a[3]/text()"
                ).get()
            )

            street_address = item.xpath(
                "//body/div[1]/div[11]/div/div[1]/div[1]/p/text()[3]"
            ).get()
            self.link_list["street_address"].append(street_address)

            # yield self.link_list

        a = {
            "title": self.link_list["title"],
            "price": self.link_list["price"],
            "number_of_room": self.link_list["number_of_rooms"],
            "apt_size": self.link_list["apt_size"],
            "district": self.link_list["district"],
            "city": self.link_list["city"],
            "street_address": self.link_list["street_address"],
        }
        file_export = pd.DataFrame(data=a)
        file_export.drop_duplicates(subset=["title"]).to_csv(
            "src/data/apt_data.csv", index_label="id"
        )
