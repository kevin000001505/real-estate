import scrapy
import json
import logging
import redis
from pyquery import PyQuery
from scrapy_redis.spiders import RedisSpider
from ..settings import settings

redis_pool_0 = redis.ConnectionPool(
    host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0
)


def get_cats(response):
    dom = PyQuery(response.text)
    item_d = dom("div.link-label:contains('熱門縣市售屋')+div a").items()
    for item in item_d:
        path = item.attr("href")[-1]
        category = item.text()
        yield category, path

def generate_pagination_url(self, category, path):
    pagination_url = "https://sale.591.com.tw/home/search/list-v2?type=2&category={path}&regionid={path}&firstRow={page}"
    for page in range(2):
        params = {"path": path, "page": page*30}
        request_headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'X-CSRF-TOKEN': '1Y6NzGPD4NvBXBw2dOgVEskHpwuOYJaIP4luAkke',
            'shType': 'list',
            'recom_community': '1',
        }
        url = pagination_url.format(**params)
        yield scrapy.Request(
            url=url, 
            callback=self.parse_links,
            dont_filter=True,
            headers=request_headers,
            meta = {
                "category": category
            })


class ExtractFirstPage(scrapy.Spider):
    name = "ExtractFirstPage"
    start_urls = ["https://sale.591.com.tw/"]
    allowed_domains = ['market.591.com.tw', 'bff.591.com.tw', 'sale.591.com.tw']

    def __init__(self):
        self.reclient = redis.Redis(host="localhost", port=6379)
        pass

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.extract_category_link,
            dont_filter=True,
        )

    def extract_category_link(self, response):
        cat_pair = get_cats(response)
        cat_pair = list(cat_pair)
        logging.debug(cat_pair)
        for item in cat_pair:
            category, path = item
            yield from generate_pagination_url(self, category, path)
    
    def parse_links(self, response):
        raw = response.json()
        if raw['status'] == 0:
            return logging.debug("No Data Found")
        item_d = raw['data']['house_list']
        for item in item_d:
            pageId = item['houseid']
            link = f"https://newhouse.591.com.tw/{pageId}"
            self.reclient.sadd('links', link)

class HousingInfo(RedisSpider):
    name = "HousingInfo"
    redis_key = 'links'

    def __init__(self):
        pass

    def parse(self, response):
        
        pass


