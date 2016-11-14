# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json

import datetime
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
    def __init__(self, mongo_uri, mongo_db, mongo_coupon_collection, mongo_seq_collection):
        self.logger = logging.getLogger(__name__)

        self.client = pymongo.MongoClient(mongo_uri)
        self.coupon_collection = self.client[mongo_db][mongo_coupon_collection]
        self.seq_collection = self.client[mongo_db][mongo_seq_collection]
        # self.coupon_collection.create_index([("selid", pymongo.ASCENDING), ("nick", pymongo.ASCENDING)], unique=True, background=True)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            mongo_coupon_collection=crawler.settings.get('MONGO_COUPON_COLLECTION'),
            mongo_seq_collection=crawler.settings.get('MONGO_SEQ_COLLECTION')
        )

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, coupon_item, spider):
        selid = coupon_item['selid']
        seller = self.coupon_collection.find_one(filter={'selid': selid}, max_time_ms=100)

        # 店铺的优惠券已经存在, 则立即update优惠券
        if seller:
            coupons = seller['coupons'] + coupon_item['coupons']
            coupons_map = dict()
            for coupon in coupons:
                acId = coupon['acId']
                coupons_map[acId] = coupon
            self.coupon_collection.update({'selid': selid}, {'$set': {'coupons': coupons_map.values(), 'mtime': datetime.datetime.now()}})

        # 店铺的优惠券不存在, 则先获取_id, 然后插入该店铺的优惠券
        else:
            coupon_item['_id'] = self.seq_collection.find_and_modify({'type': 'coupon'}, {'$inc': {'seq': 1}})['seq']
            coupon_item['ctime'] = coupon_item['mtime'] = datetime.datetime.now()
            self.coupon_collection.insert_one(dict(coupon_item))

        return coupon_item
