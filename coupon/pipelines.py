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
        # 获取(爬取到的)优惠券coupon_item对应的店铺在数据库中的数据
        selid = coupon_item['selid']
        acId = coupon_item['coupons'][0]['acId']
        data = self.coupon_collection.find_one(filter={'selid': selid}, max_time_ms=300)

        # 爬取到的优惠券对应的店铺已经存在于数据库中, 则update优惠券
        if data:
            # 爬取到的优惠券已经存在于数据库中, 则更新type和spids字段
            changed = False
            acIds = [coupon['acId'] for coupon in data['coupons']]
            if acId in acIds:
                # 爬取到优惠券在所有优惠券中的索引
                index = acIds.index(acId)

                # 爬取到的优惠券的公开/隐藏属性(newcome_type)和数据库中已存在的公开/隐藏属性(existed_type)
                newcome_type = coupon_item['coupons'][0]['type']
                existed_type = data['coupons'][index]['type']

                # 爬取到的优惠券的适用范围(newcome_spids)和数据库中已存在的适用范围(existed_spids)
                newcome_spids = coupon_item['coupons'][0]['spids']
                existed_spids = data['coupons'][index]['spids']

                # 更新优惠券的公开/隐藏属性
                if (newcome_type == 1) and (existed_type == 0):
                    data['coupons'][index]['type'] = newcome_type
                    changed = True

                # 更新优惠券的单品/通用属性
                if newcome_spids != ["-1"]:
                    if existed_spids != ["-1"]:
                        union_spids = list(set(existed_spids) | set(newcome_spids))
                        data['coupons'][index]['spids'] = union_spids
                        changed = True if len(union_spids) != len(existed_spids) else False
                    else:
                        data['coupons'][index]['spids'] = newcome_spids
                        changed = True
            # 爬取到的优惠券不在数据库中, 则添加新优惠券
            else:
                data['coupons'] += coupon_item['coupons']
                changed = True

            # update
            if changed:
                self.logger.info("update the new coupon. selid: %s, nick: %s, acId: %s, spid: %s" %
                            (selid, coupon_item['nick'], coupon_item['coupons'][0]['acId'], coupon_item['coupons'][0]['spids'][0]))
                self.coupon_collection.update({'selid': selid}, {'$set': {'coupons': data['coupons'], 'mtime': datetime.datetime.now()}})
            else:
                self.logger.info("not update the new coupon. selid: %s, nick: %s, acId: %s, spid: %s" %
                            (selid, coupon_item['nick'], coupon_item['coupons'][0]['acId'], coupon_item['coupons'][0]['spids'][0]))

        # 店铺的优惠券不存在, 则先获取_id, 然后插入该店铺的优惠券
        else:
            self.logger.info("insert the new coupon. selid: %s, nick: %s, acId: %s, spid: %s" %
                            (selid, coupon_item['nick'], coupon_item['coupons'][0]['acId'], coupon_item['coupons'][0]['spids'][0]))
            coupon_item['_id'] = self.seq_collection.find_and_modify({'type': 'coupon'}, {'$inc': {'seq': 1}})['seq']
            coupon_item['ctime'] = coupon_item['mtime'] = datetime.datetime.now()
            self.coupon_collection.insert_one(dict(coupon_item))

        return coupon_item
