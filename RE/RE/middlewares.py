# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter

# class HouseCookies(object):
#     def process_request(self, request, spider):
#         if "search" in request.url:
#             request.cookies = {
#                 '_clsk': 'naws1f%7C1722931928265%7C49%7C0%7Ct.clarity.ms%2Fcollect',
#                 'XSRF-TOKEN': 'eyJpdiI6IjMvaUpYV3hQWGFLT3ZoOCtWSkluSGc9PSIsInZhbHVlIjoidkFGaHcwMy9uREQxeU5waDdZUDZvN0d0dmk0SlNmM2IvcjdTVnY1UmRiak9pT3RRQTgyWmJEOE9qbVZqNVlKWmdBZENNWlVnc0d5NDJJLzlzWlo4a25HZlRWU1V5OHRGVG5XekNwT2NYbHdVSlRqcEhPSEFJY3JwRkp5aEtzMUQiLCJtYWMiOiI5N2EyYjExMWRlMzhmM2VkNDdkNTE3NTczMTA2NjFmYzJkYmE4MzVmNjQzNTdhNjFlODRmYTJkNWU5NGJkNzVmIiwidGFnIjoiIn0%3D',
#                 '591_new_session': 'eyJpdiI6IlhlVVNmVHJUeTNhQ3hYR3pYMmRVbFE9PSIsInZhbHVlIjoiL2pJdmFsZC9YaG9RcnZiOTUvYmRDT2FlK0FBV0Zjd1hLVkQxdnZtazJkdEdLV2d0a3JFREUyM2wxeU5WbmRoWExsZHpaTFg5TXA4N1RrRGgrQWZwbFlrckhDbmx0WGdMSHV5RW9sSElOQVpxcjdSajE4OTljQ1NKbVVWaUJ4OGYiLCJtYWMiOiJiNWYxN2U0YTU0MzZmYzEzOWI2OTk0MjNiYWZhZWQwYmUyZTY0Nzg1NTkzNjNhNzE3MmY3Njc5NTk4NTE2ZTMwIiwidGFnIjoiIn0%3D',
#             }


class ReSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ReDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
