import requests
import scrapy

class ImmoSpider(scrapy.Spider):
    name = 'immolaendle'
    start_urls = ['https://www.laendleimmo.at/mietobjekt/wohnung']

    # TODO
    def parse(self, response):
        for title in response.css('.oxy-post-title'):
            yield {'title': title.css('::text').get()}

        for next_page in response.css('a.next'):
            yield response.follow(next_page, self.parse)
