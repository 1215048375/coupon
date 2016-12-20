import json
import pymongo

if __name__ == '__main__':
    MONGO_URI = 'mongodb://spider:spiderpasw@10.0.0.95:27017/tts_spider_deploy?authMechanism=SCRAM-SHA-1'
    MONGO_DATABASE = 'tts_spider_deploy'
    MONGO_COUPON_COLLECTION = 't_spider_product_coupon'

    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COUPON_COLLECTION]

    projection = {'_id': False, 'selid': True, 'nick': True}
    with open('../../data/sellers.mongodb.json', 'w') as fin:
        for obj in collection.find(projection=projection, no_cursor_timeout=True):
            sellerId = obj['selid']
            nick = obj['nick']
            data = json.dumps({'sellerId': sellerId, 'nick': nick})
            fin.write(data + '\n')