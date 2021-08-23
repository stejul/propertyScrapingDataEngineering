import scrapy
import random
import pandas as pd

class ImmoSpider(scrapy.Spider):
    name = 'immolaendle_apt_collector'
    start_urls = ['https://www.laendleimmo.at/mietobjekt/wohnung']

    custom_settings = {
            "AUTOTHROTTLE_ENABLED": True,
            "FEED_EXPORT_ENCODING":'utf-8-sig',
            "DOWNLOADER_MIDDLEWARE": {
                'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
                'scrapy_useragents.downloadermiddlewares.useragents.UserAgentsMiddleware': 500,
                },
            "USER_AGENTS": [
                ('Mozilla/5.0 (X11; Linux x86_64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/57.0.2987.110 '
                'Safari/537.36'),  # chrome
                ('Mozilla/5.0 (X11; Linux x86_64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/61.0.3163.79 '
                'Safari/537.36'),  # chrome
                ('Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) '
                'Gecko/20100101 '
                'Firefox/55.0')  # firefox
                ],
            }

    link_list = {"link": []}

    def parse(self, response):
        for item in response.css("div.list-content h2.title-block-mobile"):
            self.link_list["link"].append(f"https://www.laendleimmo.at{item.css('a.js-ad-click::attr(href)').get()}")
            yield {"link": item.css("a.js-ad-click::attr(href)").get()}

        for next_page in response.css("ul li.next a"):
            yield response.follow(next_page, self.parse)
        
        df = pd.DataFrame(data=self.link_list)
        df.to_csv("src/data/apt_dump.csv", index_label="id")
