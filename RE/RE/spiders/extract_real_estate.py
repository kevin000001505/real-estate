import scrapy
import json
import logging
import math
import redis
from pyquery import PyQuery
from scrapy_redis.spiders import RedisSpider
from .. import settings

redis_pool_0 = redis.ConnectionPool(
    host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0
)
redis_pool_1 = redis.ConnectionPool(
    host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=1
)


def get_cats(response: scrapy.http.Response):
    dom = PyQuery(response.text)
    item_d = dom("div.link-label:contains('熱門縣市售屋')+div a").items()
    for item in item_d:
        path = item.attr("data-text")
        category = item.text()
        yield category, path


def generate_pagination_url(self, category, path):
    pagination_url = "https://bff-market.591.com.tw/v1/search/list?page={page}&regionid={path}&from=3"
    for page in range(30):
        params = {"path": path, "page": page}
        url = pagination_url.format(**params)
        yield scrapy.Request(
            url=url,
            callback=self.parse_links,
            dont_filter=True,
            meta={"category": category},
        )


class ExtractFirstPage(scrapy.Spider):
    name = "ExtractFirstPage"
    start_urls = ["https://sale.591.com.tw/"]
    allowed_domains = ["market.591.com.tw", "bff.591.com.tw", "sale.591.com.tw"]

    def __init__(self):
        self.reclient = redis.Redis(host="localhost", port=6379)
        pass

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.extract_category_link,
            dont_filter=True,
        )

    def extract_category_link(self, response: scrapy.http.Response):
        cat_pair = get_cats(response)
        cat_pair = list(cat_pair)
        logging.debug(cat_pair)
        for item in cat_pair:
            category, path = item
            yield from generate_pagination_url(self, category, path)

    def parse_links(self, response: scrapy.http.Response):
        rclient_0 = redis.StrictRedis(connection_pool=redis_pool_0)
        rclient_1 = redis.StrictRedis(connection_pool=redis_pool_1)
        raw = response.json()
        if raw["status"] == 0:
            yield logging.debug("No Data Found")
        else:
            item_d = raw["data"]["items"]
            for item in item_d:
                pageId = item["id"]
                link = f"https://market.591.com.tw/{pageId}"
                logging.debug(f"Link {link}")
                rclient_0.sadd("links", link)
                rclient_1.hset(link, "Id", pageId)
                rclient_1.hset(link, "data", json.dumps(item))
                yield item


class HousingInfo(RedisSpider):
    name = "HousingInfo"
    redis_key = "links"

    def __init__(self):
        pass

    def parse(self, response):
        rclient_1 = redis.StrictRedis(connection_pool=redis_pool_1)
        pageId = rclient_1.hget(response.url, "Id")
        pageId = pageId.decode("utf-8")
        data = rclient_1.hget(response.url, "data")
        data = json.loads(data)
        total_page = int(PyQuery(response.text)(".realprice .desc span").text().replace("筆", ""))
        total = math.ceil(total_page / 50)
        for page in range(1, total):
            url = f"https://bff-market.591.com.tw/v1/price/list?community_id={pageId}&page={page}&page_size=50"
            yield scrapy.Request(
                url=url, 
                callback=self.parse_items,
                dont_filter=True, 
                meta={"data": data}
            )
    
    def parse_items(self, response: scrapy.http.Response):
        item_d = response.json()['data']['items']
        if item_d:
            for item in item_d:
                yield item
        