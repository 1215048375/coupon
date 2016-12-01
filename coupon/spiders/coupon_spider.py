# -*- coding: utf-8 -*-
"""
该程序采集淘宝联盟List页，从中获取所有的店铺名和店铺id, 再通过淘客助手的接口获取优惠券的活动id.
"""
import random
import pymongo
import scrapy
import json
import time
import re
from coupon.items import CouponItem

# 卖家信息文件
SELLERS_FILE = './data/sellers.20161117.json'


# 从文件中读取卖家信息
def get_sellers():
    sellers = []
    with open(SELLERS_FILE, 'r') as fin:
        for line in fin:
            try:
                seller = json.loads(line.strip())
                sellers.append(seller)
            except Exception, e:
                print 'parse json in path error(%s), %s'.format(e, line.strip())
    return sellers


# 助手headers
zhushou_headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Connection': 'keep-alive',
    'Host': 'zhushou3.taokezhushou.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
}

# 淘宝headers
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


class CouponSpider(scrapy.Spider):
    name = "coupon_spider"
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DNS_TIMEOUT': 10,
        'DOWNLOAD_TIMEOUT': 60,
        # 'DOWNLOAD_DELAY': 0.15,
        'CONCURRENT_REQUESTS': 16,
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 503, 504, 400, 403, 404, 408],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'coupon.middlewares.RandomHttpProxyMiddleware': 100,
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
        },
        'ITEM_PIPELINES': {
            'coupon.pipelines.CouponPipeline': 300,
        },

        # 线上mongodb数据库
        'MONGO_URI': '199.155.122.32:27018',
        'MONGO_DATABASE': 'tts_spider_deploy',
        'MONGO_PRODUCT_COLLECTION': 't_spider_product_info2',
        'MONGO_COUPON_COLLECTION': 't_spider_product_coupon',
        'MONGO_SEQ_COLLECTION': 't_spider_product_seq',
    }

    # 连接mongodb商品表
    mongo_client = pymongo.MongoClient(custom_settings['MONGO_URI'])
    mongo_db = mongo_client[custom_settings['MONGO_DATABASE']]
    mongo_product_collection = mongo_db[custom_settings['MONGO_PRODUCT_COLLECTION']]

    sellers = get_sellers()  # 从文件中载入所有seller信息
    seller_num = 0  # 抓取到优惠券的seller数量
    coupon_num = 0  # 抓取到的优惠券数量
    session = ''    # 爬虫链接的会话

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CouponSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.logger.info('Spider closed: %s, and crawled %s sellers, success %s sellers, total %s coupons' \
                         % (spider.name, len(self.sellers), self.seller_num, self.coupon_num))

    def start_requests(self):
        """重载父类的start_request方法, 建立会话"""
        url = 'http://zhushou3.taokezhushou.com/api/v1/getdata?itemid=0&version=3.5.1'
        yield scrapy.Request(url=url, callback=self.parse_session)

    def parse_session(self, response):
        """ 从广告的响应中获取cookie, 用该cookie来请求第一个店铺的优惠券数据
        :param response: 广告请求的响应
        """
        # response对应的reqeust中没有cookie: 从response中获得session, xsrf_token, taokezhushou_plugin
        self.session = response.headers.getlist('Set-Cookie')[0].split(';')[0]
        xsrf_token = response.headers.getlist('Set-Cookie')[1].split(';')[0]
        taokezhushu_plugin = response.headers.getlist('Set-Cookie')[2].split(';')[0]

        # 更新cookie
        cookie = self.session + ';' + xsrf_token + ';' + taokezhushu_plugin
        zhushou_headers['Cookie'] = cookie

        # 用获取到cookie, 获取第一个店铺的广告/优惠券数据
        seller_index = 0
        seller = self.sellers[seller_index]
        url = 'http://zhushou3.taokezhushou.com/api/v1/getdata?itemid={0:d}&version=3.5.1'.format(random.randint(1000000, 99999999))
        yield scrapy.Request(url=url, meta={'seller_index': seller_index}, headers=zhushou_headers, callback=self.parse_ad)
        url = 'http://zhushou3.taokezhushou.com/api/v1/coupons_base/{0:s}?item_id={1:d}'.format(seller['sellerId'], random.randint(1000000, 99999999))
        yield scrapy.Request(url=url, meta={'seller_index': seller_index}, headers=zhushou_headers, callback=self.parse_acIds)

    def parse_ad(self, response):
        pass

    def parse_acIds(self, response):
        """ 解析一个店铺的所有优惠活动id, 然后从响应中获取cookie, 用该cookie来请求下一个店铺的广告/优惠券数据
        :param response: 优惠券请求的响应
        """
        # 获取从淘客助手返回的优惠券数据对应的卖家
        seller_index = response.meta['seller_index']
        seller = self.sellers[seller_index]

        # 从response中获得xsrf_token, taokezhushou_plugin, 然后更新cookie中已有session对应的cookie
        xsrf_token = response.headers.getlist('Set-Cookie')[0].split(';')[0]
        taokezhushu_plugin = response.headers.getlist('Set-Cookie')[1].split(';')[0]
        cookie = self.session + ';' + xsrf_token + ';' + taokezhushu_plugin
        zhushou_headers['Cookie'] = cookie

        # 处理优惠券数据
        try:
            # 解析从淘客助手的返回的优惠券数据
            data = json.loads(response.body)
            if data['data']:
                if data['data'][0] is not None:
                    # 统计拿到优惠券的店铺数量
                    self.seller_num += 1

                    # 获取淘客助手返回的优惠券id, 及其对应的卖家信息, 然后调用start_check函数检测
                    nick = seller['nick']
                    selid = seller['sellerId']
                    acIds = [item['activity_id'] for item in data['data']]

                    # 查找该店铺的多个CPS商品的spid, 对优惠券分别用多个spid检测是否店铺优惠券.
                    cps_spids = list(self.mongo_product_collection.find(filter={'nick': nick, 'isCps': 1}, projection={'_id': False, 'spid': True}).limit(20))
                    cps_spids = [cps_spid['spid'] for cps_spid in cps_spids]

                    # 两种情况对应的处理方式:
                    #   1. 未获得该店铺CPS商品spid的情况: 默认优惠券适用范围未知, 然后调用parse_coupon函数解析优惠券信息
                    #   2. 获得了该店铺CPS商品spid的情况: 默认优惠券适用范围未知, 然后调用check函数检测其是否店铺优惠券
                    if not cps_spids:
                        for acId in acIds:
                            self.logger.error('selid: %s, nick: %s, acId: %s. can not find cps product for detection' % (selid, nick, acId))
                            meta = {
                                'is_proxy': True,   # ADSL代理
                                'selid': selid,     # 当前优惠券所属的卖家id
                                'nick': nick,       # 当前优惠券所属的卖家旺旺号
                                'acId': acId,       # 当前优惠券id
                                'spids': [-1]       # 当前优惠券适用的商品范围
                            }
                            url = 'http://shop.m.taobao.com/shop/coupon.htm?seller_id={0:s}&activity_id={1:s}'.format(selid, acId)
                            yield scrapy.Request(url=url, meta=meta, headers=taobao_headers, callback=self.parse_coupon)
                    else:
                        for acId in acIds:
                            cps_spid_index = 0
                            meta = {
                                'is_proxy': True,                   # ADSL代理
                                'selid': selid,                     # 当前优惠券所属的卖家id
                                'nick': nick,                       # 当前优惠券所属的卖家旺旺号
                                'acId': acId,                       # 当前优惠券id
                                'spids': [-1],                      # 当前优惠券适用的商品范围, 默认为[-1], 即适用范围未知
                                'cps_spids': cps_spids,             # 检测当前优惠券是否店铺券的多个(CPS商品)spid
                                'cps_spid_index': cps_spid_index    # 当前检测当前优惠券是否店铺券的spid在cps_spids中的索引
                            }
                            url = 'http://uland.taobao.com/cp/coupon?activityId={0:s}&itemId={1:s}'.format(acId, cps_spids[cps_spid_index])
                            yield scrapy.Request(url=url, meta=meta, callback=self.check)
                else:
                    # 淘客助手存在该店铺的优惠券数据, 但其反作弊手段导致无数据返回
                    self.logger.error('%sth sellerId: %s, activities exist, but return nothing. %s' % (seller_index + 1, seller['sellerId'], response.url))
            else:
                # 淘客助手不存在该店铺的优惠券数据
                self.logger.error('%sth sellerId: %s, activities not exist. %s' % (seller_index + 1, seller['sellerId'], response.url))
        except Exception, e:
            # 淘客助手返回的数据不能解析成json格式, 一般是因为其防攻击策略, 则休眠一段时间重新请求受限的请求
            self.logger.error('restricted access. %sth sellerId: %s, session: %s, %s' % (seller_index + 1, seller['sellerId'], self.session, response.url))
            time.sleep(1)
            seller_index -= 1

        # 获取下一个店铺的广告/优惠券数据
        seller_index += 1
        if seller_index < len(self.sellers):
            seller = self.sellers[seller_index]
            url = 'http://zhushou3.taokezhushou.com/api/v1/getdata?itemid={0:d}&version=3.5.1'.format(random.randint(1000000, 99999999))
            yield scrapy.Request(url=url, meta={'seller_index': seller_index}, headers=zhushou_headers, callback=self.parse_ad, dont_filter=True)
            url = 'http://zhushou3.taokezhushou.com/api/v1/coupons_base/{0:s}?item_id={1:d}'.format(seller['sellerId'], random.randint(1000000, 99999999))
            yield scrapy.Request(url=url, meta={'seller_index': seller_index}, headers=zhushou_headers, callback=self.parse_acIds, dont_filter=True)
        else:
            pass

    def check(self, response):
        """ 检测商品是否店铺券
        :param response: 对商品和优惠券二合一时，淘宝给出的响应
        """
        # 获取response中的数据
        meta = response.meta
        selid = meta['selid']
        nick = meta['nick']
        acId = meta['acId']
        spids = meta['spids']
        cps_spids = meta['cps_spids']
        cps_spid_index = meta['cps_spid_index']

        # 将淘宝接口的返回结构解析成json格式, 并提取出retStatus
        response_ = json.loads(response.body)
        retStatus = response_['result']['retStatus']

        # 两种情况对应的处理方式:
        #   1. (retStatus!=0)并且(cps_spid_index+1<len(cps_spids))的情况: 该情况可能是由于商品下架/优惠券是单品券导致的, 故使用下一个(CPS商品)spid调用check函数继续检测
        #   2. (retStatus==0)或(cps_spid_index+1>len(cps_spids))的情况又可细分为两种处理方式: 区别在于设置优惠券的适用商品范围, 然后调用parse_coupon解析优惠券信息并入库
        #       2.1. retStatus==0的情况: 表明成功检测到优惠券为店铺券, 则设置优惠券适用的商品范围为全店铺
        #       2.2. cps_spid_index+1>len(cps_spids)的情况: 表明用所有(CPS商品)spid检测都不存在retStatus为0(即为店铺券)的情况, 则设置优惠券适用范围为meta中传入的适用范围
        if (retStatus != 0) and (cps_spid_index + 1 < len(cps_spids)):
            # 1. (retStatus!=0)并且(cps_spid_index+1<len(cps_spids))的情况: 该情况可能是由于商品下架/优惠券是单品券导致的, 故使用下一个(CPS商品)spid调用check函数继续检测
            cps_spid_index += 1
            meta['cps_spid_index'] = cps_spid_index
            url = 'http://uland.taobao.com/cp/coupon?activityId={0:s}&itemId={1:s}'.format(acId, cps_spids[cps_spid_index])
            yield scrapy.Request(url=url, meta=meta, callback=self.check)
        else:
            # 2. (retStatus==0)或(cps_spid_index+1>len(cps_spids))的情况又可细分为两种处理方式
            if retStatus == 0:
                meta['spids'] = []
                self.logger.info('sellerId: %s, nick: %s, acId: %s, spid: %s, coupon is for shop' % (selid, nick, acId, cps_spids[cps_spid_index]))
            else:
                meta['spids'] = spids
                self.logger.info('sellerId: %s, nick: %s, acId: %s, spid: %s, coupon is for product' % (selid, nick, acId, spids[0]))

            # 调用start_parse_coupon解析优惠券并入口
            url = 'http://shop.m.taobao.com/shop/coupon.htm?seller_id={0:s}&activity_id={1:s}'.format(selid, acId)
            yield scrapy.Request(url=url, meta=meta, headers=taobao_headers, callback=self.parse_coupon)

    def parse_coupon(self, response):
        """ 解析领券页面,获取优惠券的详细信息
        :param response: 领券页面的响应
        """
        # 获取response中的数据
        meta = response.meta
        selid = meta['selid']
        nick = meta['nick']
        acId = meta['acId']
        spids = meta['spids']

        # 从response返回的优惠券页面中解析优惠券信息
        try:
            # 解析优惠券信息
            price = response.xpath('//*[@class="coupon-info"]/dl/dt/text()').extract()[0].split(u'元')[0]
            price = int(float(price) * 100)
            condition = response.xpath('//*[@class="coupon-info"]/dl/dd[2]/text()').extract()[0]
            mprice = re.match(u".*满([0-9]+(\\.[0-9]+)?)", condition).group(1)
            mprice = int(float(mprice) * 100)
            period_of_validity = response.xpath('//*[@class="coupon-info"]/dl/dd[3]/text()').extract()[0]
            start = re.match(u".*:(.*)至(.*)", period_of_validity).group(1).replace('-', '.')
            end = re.match(u".*:(.*)至(.*)", period_of_validity).group(2).replace('-', '.')

            # 为优惠券存入数据库时的必需字段赋值
            coupon = {
                'acId': acId,
                'type': 1,
                'price': price,
                'condition': condition,
                'mprice': mprice,
                'start': start,
                'end': end,
                'spids': spids,
            }

            # 构造CouponItem类型变量用来将优惠券写入数据库
            coupon_item = CouponItem()
            coupon_item['selid'] = selid
            coupon_item['nick'] = nick
            coupon_item['coupons'] = [coupon]
            self.logger.info('sellerId: %s, nick: %s, acId: %s, condition: %s' % (selid, nick, acId, condition))

            # 统计优惠券数量
            self.coupon_num += 1

            # 将coupon_item抛出给pipeline处理
            yield coupon_item
        except Exception, e:
            # 解析优惠券信息异常
            self.logger.error("parse coupon fail(%s), %s" % (e, response.url))
            pass
