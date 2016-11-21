# coding=utf-8
import datetime
import pymongo

MONGO_URI = '199.155.122.32:27018'
MONGO_DATABASE = 'tts_spider_deploy'
MONGO_COUPON_COLLECTION = 't_spider_product_coupon'

if __name__ == '__main__':
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COUPON_COLLECTION]
    
    today = datetime.datetime.now()
    projection = {'_id': False, 'selid': True, 'coupons': True}
    for obj in collection.find(projection=projection, no_cursor_timeout=True):
        # 找到过期的优惠券在数组中的下标
        remove_indeces = []
        for index, coupon in enumerate(obj['coupons']):
            end = datetime.datetime.strptime(coupon['end'], '%Y.%m.%d')
            if end < today:
                remove_indeces.append(index)
            else:
                pass

        # 删除本地数组中的过期优惠券
        coupons = [coupon for index, coupon in enumerate(obj['coupons']) if index not in remove_indeces]

        # 更新优惠券
        if len(coupons) == len(obj['coupons']):
            # 没有删除优惠券, 则不做任何操作
            pass
        elif coupons:
            # 删除了部分优惠券, 则更新'coupons'字段
            collection.update_one({'selid': obj['selid']}, {'$set': {'coupons': coupons}})
        else:
            # 删除了所有优惠券, 则删除doc
            collection.delete_one({'selid': obj['selid']})