# -*- coding: utf-8 -*-

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())
# process.crawl('seller_spider')
process.crawl('coupon_spider')
process.start()
