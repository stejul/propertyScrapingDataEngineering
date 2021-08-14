import scrapy

class ImmoSpider(scrapy.Spider):
    name = 'immolaendle'
    start_urls = ['https://www.laendleimmo.at/mietobjekt/wohnung']
    custom_settings = {
            "AUTOTHROTTLE_ENABLED": True,
            "FEED_EXPORT_ENCODING":'utf-8-sig'
            }

    # TODO
    """
    def parse(self, response):
        for item in response.css("div.obj-cls"):
            yield {
                    "title": item.css("a.js-ad-click::text").get(),
                    "bezirk": item.css("div.list_search div.list-content p.regular a::text")[0].get(),
                    "stadt": item.css("div.list_search div.list-content p.regular a::text")[1].get(),
                    "size": item.css("div.list-content p::text")[1].get(),
                    "price": item.css("div.price-right-mobile::text").get()
                    }
        
        for next_page in response.css("ul li.next a"):
            yield response.follow(next_page, self.parse)
    """
