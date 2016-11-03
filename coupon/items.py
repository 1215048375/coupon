# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SellerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    sellerId = scrapy.Field()
    shopTitle = scrapy.Field()


class CouponItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    selid = scrapy.Field()
    wsite = scrapy.Field()
    coupons = scrapy.Field()