# -*- coding: utf-8 -*-
"""
该程序采集淘宝联盟List页，从中获取所有的店铺名和店铺id, 再通过淘客助手的接口获取优惠券的活动id.
"""
import scrapy
import json
from pybloom import ScalableBloomFilter
from coupon.items import CouponItem


SELLERS_FILE = './sellers.20161104.json'


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


class CouponSpider(scrapy.Spider):
    name = "coupon_spider"
    start_urls = []
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DNS_TIMEOUT': 10,
        'DOWNLOAD_TIMEOUT': 60,
        'DOWNLOAD_DELAY': 0.1,
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

        # 线下
        # 'MONGO_URI': '199.155.122.32:27018',
        # 'MONGO_DATABASE': 'coupons',
        # 'MONGO_COLLECTION': 'coupons_20161104'

        # 线上
        'MONGO_URI': 'mongodb://spider:spiderpasw@10.0.0.93:27017/tts_spider_deploy?authMechanism=SCRAM-SHA-1',
        'MONGO_DATABASE': 'tts_spider_deploy',
        'MONGO_COLLECTION': 't_spider_product_coupon'
    }

    zhushou_headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'zhushou3.taokezhushou.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
    }

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

    filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
    sellers = get_sellers()
    seller_num = 0
    coupon_num = 0
    session = ''

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CouponSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.logger.info('Spider closed: %s, and crawled %s sellers, success %s sellers, total %s coupons'\
                         % (spider.name, len(self.sellers), self.seller_num, self.coupon_num))

    def start_requests(self):
        """重载父类的start_request方法, 请求的目地是建立会话, 获取cookie"""
        url = 'http://zhushou3.taokezhushou.com/api/v1/getdata?itemid=&version=3.5.1'
        yield scrapy.Request(url=url, callback=self.parse_session)

    def parse_session(self, response):
        """ 从广告的响应中获取cookie, 用该cookie来请求第一个店铺的优惠券数据
        :param response: 广告请求的响应
        """
        # 获取带有会话的cookie
        self.session = response.headers.getlist('Set-Cookie')[0].split(';')[0]
        xsrf_token = response.headers.getlist('Set-Cookie')[1].split(';')[0]
        taokezhushu_plugin = response.headers.getlist('Set-Cookie')[2].split(';')[0]
        cookie = self.session + ';' + xsrf_token + ';' + taokezhushu_plugin
        self.zhushou_headers['Cookie'] = cookie

        # 用获取到的cookie获取第一个店铺的广告/优惠券数据
        serller_index = 0
        seller = self.sellers[serller_index]
        url = 'http://zhushou3.taokezhushou.com/api/v1/getdata?itemid={0:d}&version=3.5.1'.format(seller['sellerId'])
        yield scrapy.Request(url=url, meta={'serller_index': serller_index}, headers=self.zhushou_headers, callback=self.parse_ad)
        url = 'http://zhushou3.taokezhushou.com/api/v1/coupons_base/{0:d}?item_id={0:d}'.format(seller['sellerId'])
        yield scrapy.Request(url=url, meta={'serller_index': serller_index}, headers=self.zhushou_headers, callback=self.parse_acIds)

    def parse_ad(self, response):
        pass

    def parse_acIds(self, response):
        """ 解析一个店铺的所有优惠活动id, 然后从响应中获取cookie, 用该cookie来请求下一个店铺的广告/优惠券数据
        :param response: 优惠券请求的响应
        """
        # 获取response中的数据
        serller_index = response.meta['serller_index']
        seller = self.sellers[serller_index]

        # 解析优惠券活动id
        try:
            data = json.loads(response.body)
            if data['data']:
                if data['data'][0] is not None:
                    # 获取一个店铺中的所有优惠券页面, 解析优惠券信息
                    activity_ids = [item['activity_id'] for item in data['data']]
                    coupon_item = CouponItem()
                    coupon_item['selid'] = str(seller['sellerId'])
                    coupon_item['wsite'] = 'taobao'
                    coupon_item['coupons'] = []
                    meta = {
                        'sellerId': seller['sellerId'],
                        'serller_index': serller_index,
                        'activity_ids': activity_ids,
                        'coupon_item': coupon_item,
                        'activity_index': 0,
                    }
                    self.seller_num += 1
                    url = 'http://shop.m.taobao.com/shop/coupon.htm?seller_id={0:d}&activity_id={1:s}'.format(seller['sellerId'], activity_ids[0])
                    self.logger.info('%sth sellerId: %s, activities: %s, %s, %s' % (serller_index + 1, seller['sellerId'], len(data['data']), response.url, data['data']))
                    yield scrapy.Request(url=url, meta=meta, headers=self.taobao_headers, callback=self.parse_coupon)
                else:
                    self.logger.error('%sth sellerId: %s, activities exist, but return nothing. %s' % (serller_index + 1, seller['sellerId'], response.url))
            else:
                self.logger.error('%sth sellerId: %s, activities not exist. %s' % (serller_index + 1, seller['sellerId'], response.url))

            # 获取response中的cookie
            try:
                xsrf_token = response.headers.getlist('Set-Cookie')[0].split(';')[0]
                taokezhushu_plugin = response.headers.getlist('Set-Cookie')[1].split(';')[0]
                cookie = self.session + ';' + xsrf_token + ';' + taokezhushu_plugin
                self.zhushou_headers['Cookie'] = cookie

                # 用获取到的cookie获取广告/优惠券数据
                serller_index += 1
                if serller_index < len(self.sellers):
                    seller = self.sellers[serller_index]
                    url = 'http://zhushou3.taokezhushou.com/api/v1/getdata?itemid={0:d}&version=3.5.1'.format(seller['sellerId'])
                    yield scrapy.Request(url=url, meta={'serller_index': serller_index}, headers=self.zhushou_headers, callback=self.parse_ad)
                    url = 'http://zhushou3.taokezhushou.com/api/v1/coupons_base/{0:d}?item_id={0:d}'.format(seller['sellerId'])
                    yield scrapy.Request(url=url, meta={'serller_index': serller_index}, headers=self.zhushou_headers, callback=self.parse_acIds)
            except Exception, e:
                self.logger.error('cookies error(%s), %s' % (e, response.headers))
        except Exception, e:
            self.logger.error('restricted access(%s). %sth sellerId: %s, %s' % (e, serller_index + 1, seller['sellerId'], response.url))
            url = 'http://zhushou3.taokezhushou.com/api/v1/getdata?itemid={0:d}&version=3.5.1'.format(seller['sellerId'])
            yield scrapy.Request(url=url, meta={'serller_index': serller_index}, headers=self.zhushou_headers, callback=self.parse_ad)
            url = 'http://zhushou3.taokezhushou.com/api/v1/coupons_base/{0:d}?item_id={0:d}'.format(seller['sellerId'])
            yield scrapy.Request(url=url, meta={'serller_index': serller_index}, headers=self.zhushou_headers, callback=self.parse_acIds)

    def parse_coupon(self, response):
        """ 解析领券页面,获取优惠券的详细信息
        :param response: 领券页面的响应
        """
        # 获取response中的数据
        sellerId = response.meta['sellerId']
        serller_index = response.meta['serller_index']
        activity_ids = response.meta['activity_ids']
        activity_index = response.meta['activity_index']
        coupon_item = response.meta['coupon_item']

        # 从页面中解析优惠券信息
        try:
            # 解析优惠券信息
            price = response.xpath('//*[@class="coupon-info"]/dl/dt/text()').extract()[0].split(u'元')[0]
            condition = response.xpath('//*[@class="coupon-info"]/dl/dd[2]/text()').extract()[0]
            period_of_validity = response.xpath('//*[@class="coupon-info"]/dl/dd[3]/text()').extract()[0]
            start = period_of_validity.split(':')[1].split(u'至')[0].replace('-', '.')
            end = period_of_validity.split(':')[1].split(u'至')[1].replace('-', '.')

            # 将优惠券信息加入coupon_item
            coupon = {
                'acId': activity_ids[activity_index],
                'condition': condition,
                'type': 0,
                'price': int(price)*100,
                'start': start,
                'end': end,
            }
            coupon_item['coupons'].append(coupon)
            self.logger.info("%sth sellerId: %s, %sth coupon success, %s" % (serller_index + 1, sellerId, activity_index + 1, response.url))
        except Exception, e:
            self.logger.error("%sth sellerId: %s, %sth coupon fail(%s), %s" % (serller_index + 1, sellerId, activity_index + 1, e, response.url))
            pass

        # 获取该店铺中的下一个优惠券
        activity_index += 1
        if activity_index < len(activity_ids):
            meta = {
                'sellerId': sellerId,
                'serller_index': serller_index,
                'activity_ids': activity_ids,
                'coupon_item': coupon_item,
                'activity_index': activity_index,
            }
            url = 'http://shop.m.taobao.com/shop/coupon.htm?seller_id={0:d}&activity_id={1:s}'.format(sellerId, activity_ids[activity_index])
            yield scrapy.Request(url=url, meta=meta, headers=self.taobao_headers, callback=self.parse_coupon)
        else:
            self.coupon_num += len(coupon_item['coupons'])
            self.logger.info("%sth sellerId: %s, activities: %s, coupons: %s" % (serller_index + 1, sellerId, len(activity_ids), len(coupon_item['coupons'])))
            yield coupon_item

