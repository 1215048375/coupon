# coding=utf-8
import re
import pymongo
import requests
from coupon.dynamicip import DynamicIP
from scrapy.selector import Selector


# 淘宝请求headers
taobao_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': 'shop.m.taobao.com',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
}


class DeleterSpider(object):
    def __init__(self, mongo_url, mongo_database, mongo_coupon_collection):
        # 连接mongodb
        self.client = pymongo.MongoClient(mongo_url)
        self.coupon_collection = self.client[mongo_database][mongo_coupon_collection]
        # adsl
        # self.dynamic_ip = DynamicIP('199.155.122.131:2181', '/adsl_proxy/lock')
        # self.dynamic_ip.run()

    def start(self):
        projection = {'_id': True, 'selid': True, 'coupons': True}
        for obj in self.coupon_collection.find(projection=projection, no_cursor_timeout=True).sort([('_id', pymongo.ASCENDING)]):
            # print '_id: {0:d}, selid: {1:s}'.format(obj['_id'], obj['selid'])
            existed_coupons = obj['coupons']
            checked_coupons = []
            for coupon in existed_coupons:
                if self.check_coupon(str(obj['selid']), coupon['acId']):
                    checked_coupons.append(coupon)

            if not checked_coupons:
                self.coupon_collection.delete_one({'_id': obj['_id']})
                print '_id: {0:d}, selid: {1:s}, delete'.format(obj['_id'], obj['selid'])
            if len(existed_coupons) != len(checked_coupons):
                self.coupon_collection.update_one({'selid': obj['selid']}, {'$set': {'coupons': checked_coupons}})
                print '_id: {0:d}, selid: {1:s}, update'.format(obj['_id'], obj['selid'])
            else:
                print '_id: {0:d}, selid: {1:s}, remain'.format(obj['_id'], obj['selid'])

    def check_coupon(self, selid, acId):
        """ 解析领券页面,获取优惠券的详细信息
        :param response: 领券页面的响应
        """
        # 获取优惠券页面
        url = 'http://shop.m.taobao.com/shop/coupon.htm?seller_id={0:s}&activity_id={1:s}'.format(selid, acId)
        # proxies = dict()
        # proxy = self.dynamic_ip.get_proxy()
        # if proxy:
        #     proxies['http'] = "http://{0:s}:3128".format(proxy)
        #     proxies['https'] = "http://{0:s}:3128".format(proxy)
        while True:
            try:
                # response = requests.get(url=url, headers=taobao_headers, proxies=proxies, verify=False)
                response = requests.get(url=url, headers=taobao_headers, verify=False)
                break
            except:
                pass

        # 从response返回的优惠券页面中解析优惠券信息
        try:
            # 解析优惠券信息
            price = Selector(text=response.text).xpath('//*[@class="coupon-info"]/dl/dt/text()').extract()[0].split(u'元')[0]
            price = int(float(price) * 100)
            condition = Selector(text=response.text).xpath('//*[@class="coupon-info"]/dl/dd[2]/text()').extract()[0]
            mprice = re.match(u".*满([0-9]+(\\.[0-9]+)?)", condition).group(1)
            mprice = int(float(mprice) * 100)
            period_of_validity = Selector(text=response.text).xpath('//*[@class="coupon-info"]/dl/dd[3]/text()').extract()[0]
            start = re.match(u".*:(.*)至(.*)", period_of_validity).group(1).replace('-', '.')
            end = re.match(u".*:(.*)至(.*)", period_of_validity).group(2).replace('-', '.')
            return True
        except Exception, e:
            # 解析优惠券信息异常
            return False

if __name__ == '__main__':
    deleter_spider = DeleterSpider(mongo_url='mongodb://spider:spiderpasw@10.0.0.166:27017/tts_spider_deploy?authMechanism=SCRAM-SHA-1',
                                   mongo_database='tts_spider_deploy',
                                   mongo_coupon_collection='t_spider_product_coupon')
    deleter_spider.start()
