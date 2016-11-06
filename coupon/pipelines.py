# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import pymongo
import logging


class SellerPipeline(object):
    def __init__(self, sellers_file):
        self.file = open(sellers_file, 'w')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(sellers_file=crawler.settings.get('SELLERS_FILE'))

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        return item


class CouponPipeline(object):
    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.client = pymongo.MongoClient(mongo_uri)
        self.collection = self.client[mongo_db][mongo_collection]
        self.coupon_items = []
        self.total_items = 0
        self.logger = logging.getLogger(__name__)
        self.collection.create_index([("selid", pymongo.ASCENDING)], unique=True, background=True)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            mongo_collection=crawler.settings.get('MONGO_COLLECTION'),
        )

    def close_spider(self, spider):
        if self.coupon_items:
            self.total_items += len(self.coupon_items)
            self.collection.insert_many(self.coupon_items)
            self.coupon_items = []
            self.logger.info("Eventually put %d coupons into mongodb " % self.total_items)
        self.client.close()

    def process_item(self, item, spider):
        # 收集到1000个item, 批量插入
        if len(self.coupon_items) >= 100:
            self.total_items += len(self.coupon_items)
            self.collection.insert_many(self.coupon_items)
            self.coupon_items = []
            self.logger.info("Put %d products into mongodb" % self.total_items)
        # 收集item
        else:
            self.coupon_items.append(dict(item))

        return item
