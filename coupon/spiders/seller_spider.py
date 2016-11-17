# -*- coding: utf-8 -*-
"""
该程序采集淘宝联盟List页，从中获取所有的店铺名和店铺id, 再通过淘客助手的接口获取优惠券的活动id.
"""
import scrapy
import datetime
import json
from pybloom import ScalableBloomFilter
from coupon.items import SellerItem


def make_url(channel='', toPage=1, perPageSize=100, sortType=3, catIds='',startTkRate='', endTkRate='', userType='',
             startPrice='', endPrice='', is_proxy=True):
    """构造url
    :param channel: 频道，默认为nzjh(女装尖货)
    :param toPage: 页索引. 默认为第1页
    :param perPageSize: 每页商品数，默认为最大值100
    :param sortType: 排序方式. 默认为3(价格从高到低排序)
    :param catIds: 类目号.
    :param startTkRate: 起始比率.
    :param endTkRate: 终止比率.
    :param userType: 店铺类型.（天猫/淘宝）
    :param startPrice: 起始价格
    :param endPrice: 终止价格
    :param is_proxy: 构造url时不使用该参数, 仅为了兼容response的meta
    """
    url = 'http://pub.alimama.com/items/channel/{0:s}.json?channel={0:s}&toPage={1:d}&perPageSize={2:d}&sortType={3:d}&catIds={4:s}&level=1&startPrice={5:s}&endPrice={6:s}&userType={7:s}&startTkRate={8:s}&endTkRate={9:s}'.format(
        channel, toPage, perPageSize, sortType, catIds, startPrice, endPrice, userType, startTkRate, endTkRate)
    return url


class SellerSpider(scrapy.Spider):
    name = "seller_spider"
    start_urls = []
    custom_settings = {
        'USER_AGENT_LIST': './UserAgents.txt',
        'ROBOTSTXT_OBEY': False,
        'DNS_TIMEOUT': 10,
        'DOWNLOAD_TIMEOUT': 60,
        'DOWNLOAD_DELAY': 0.1,
        'CONCURRENT_REQUESTS': 16,
        'COOKIES_ENABLED': False,
        'COOKIES_DEBUG': False,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 503, 504, 400, 403, 404, 408],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'coupon.middlewares.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coupon.middlewares.RandomHttpProxyMiddleware': 100,
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
        },
        'ITEM_PIPELINES': {
            'coupon.pipelines.SellerPipeline': 300,
        },
        'SELLERS_FILE': './data/sellers.{0:s}.json'.format(datetime.datetime.now().strftime("%Y%m%d"))
    }

    filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)   # 布隆过滤器, 用于过滤掉重复的卖家
    product_nums = dict()   # 每个channel的商品总数. key为channel名, value为该channel的商品总数
    seller_num = 0          # 获取到的去重后的卖家总数

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SellerSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.logger.info('Spider closed: %s, and crawled %s products, %s sellerIds' % (spider.name, self.product_nums, self.seller_num))
        pass

    def start_requests(self):
        """重载父类的start_request方法"""
        channels = ['nzjh', 'muying', 'qqhd', 'ifs', 'qbb', 'hch', 'cdj', 'jyj', 'kdc', 'diy', '9k9', '20k', 'tehui']
        for channel in channels:
            url = 'http://pub.alimama.com/items/channel/{0:s}.json?channel={0:s}'.format(channel)
            meta = {
                'channel': channel,
                'toPage': 1,
                'perPageSize': 100,
                'sortType': 3,
                'catIds': '',
                'startPrice': '',
                'endPrice': '',
                'userType': '',
                'startTkRate': '',
                'endTkRate': ''
            }
            yield scrapy.Request(url=url, meta=meta, callback=self.parse)

    def parse(self, response):
        """ 解析List页, 从中获取所有店铺名
        :param response: 获取到的List页的http响应
        """
        meta = response.meta.copy()
        meta.pop('download_timeout')
        meta.pop('download_latency')
        meta.pop('download_slot')
        meta.pop('depth')
        if 'retry_times' in meta:
            meta.pop('retry_times')

        try:
            data = json.loads(response.body)
            if not data['data']['head']['docsfound']:
                self.logger.info('no docs found in channel: %s, %s' % (meta['channel'], response.url))
            else:
                pages = data['data']['paginator']['pages']
                # pages>100的情况：
                if pages > 100:
                    # 未设置catIds的情况： 构造设置了catIds的请求
                    # if not meta['catIds']:
                    #     navigators = data['data']['navigator']
                    #     for navigator in navigators:
                    #         meta['catIds'] = str(navigator['id'])
                    #         url = make_url(**meta)
                    #         yield scrapy.Request(url=url, meta=meta, callback=self.parse)

                    # 已设置catIds的情况：
                        # 1.价格区间不是最小: 构造更小价格区间的请求;
                        # 2.价格区间已经最小:
                            # 2.1 比率区间不是最小, 构造更小比率区间的请求;
                            # 2.2 比率区间也已经最小, 准备解析
                    if not meta['catIds']:
                        # 获取当前的请求起始/终止价格, 起始/终止比率
                        startPrice = float(meta['startPrice']) if meta['startPrice'] else 0.00
                        endPrice = float(data['data']['pageList'][0]['zkPrice'])
                        startTkRate = float(meta['startTkRate']) if meta['startTkRate'] else 0.00
                        endTkRate = float(meta['endTkRate']) if meta['endTkRate'] else 100.0

                        # 1.价格区间不是最小: 构造更小价格区间的请求
                        if round(endPrice-startPrice, 2) > 0.01:
                            middlePrice = round((startPrice+endPrice)/2, 2)
                            prices = [startPrice, middlePrice, endPrice]
                            for i in range(len(prices) - 1):
                                meta['startPrice'] = str(prices[i])
                                meta['endPrice'] = str(prices[i+1])
                                url = make_url(**meta)
                                yield scrapy.Request(url=url, meta=meta, callback=self.parse)

                        # 2.价格区间已经最小:
                        if round(endPrice-startPrice, 2) <= 0.01:
                            # 2.1 比率区间不是最小, 构造更小比率区间的请求;
                            if round(endTkRate-startTkRate, 2) > 0.01:
                                self.logger.error('Prices[%s-%s] is minimum, but TkRate[%s-%s] is not minimum, %s' %\
                                                 (startPrice, endPrice, startTkRate, endTkRate, response.url))
                                middleTkRate = round((startTkRate+endTkRate)/2, 2)
                                TkRates = [startTkRate, middleTkRate, endTkRate]
                                for i in range(len(TkRates) - 1):
                                    meta['startTkRate'] = str(TkRates[i])
                                    meta['endTkRate'] = str(TkRates[i+1])
                                    url = make_url(**meta)
                                    yield scrapy.Request(url=url, meta=meta, callback=self.parse)

                            # 2.2 比率区间已经最小, 准备解析
                            if (round(endTkRate-startTkRate, 2) <= 0.01) and (meta['toPage'] == 1):
                                self.logger.error("FUCK YOU. Prices[%s-%s] is minimum, and TkRate[%s-%s] is minimum. %s" % \
                                                  (startPrice, endPrice, startTkRate, endTkRate, response.url))
                                pages = 100

                # pages<=100的情况：
                if pages <= 100:
                    # 解析出店铺信息
                    sellerIds = [info['sellerId'] for info in data['data']['pageList']]
                    nicks = [info['nick'] for info in data['data']['pageList']]
                    for i in range(len(sellerIds)):
                        if self.filter.add(sellerIds[i]):
                            pass
                        else:
                            self.seller_num += 1
                            seller_item = SellerItem()
                            seller_item['sellerId'] = sellerIds[i]
                            seller_item['nick'] = nicks[i]
                            yield seller_item
                    if meta['channel'] not in self.product_nums.keys():
                        self.product_nums[meta['channel']] = 0
                    self.product_nums[meta['channel']] += len(sellerIds)

                    # 调试
                    pageNo = data['data']['head']['pageNo']
                    self.logger.info('%s, Prices[%s-%s], TkRate[%s-%s], P%s/%s, %s, %s' % (meta['channel'], meta['startPrice'],
                        meta['endPrice'], meta['startTkRate'], meta['endTkRate'], pageNo, pages, len(sellerIds), response.url))

                    # 构造下一页请求
                    pageNo = data['data']['head']['pageNo']
                    if pageNo < pages:
                        meta['toPage'] = pageNo + 1
                        url = make_url(**meta)
                        yield scrapy.Request(url=url, meta=meta, callback=self.parse)
        except Exception, e:
            url = make_url(**meta)
            self.logger.error('restricted access(%s), %s, %s' % (e, url, response.url))
            yield scrapy.Request(url=url, meta=meta, callback=self.parse, dont_filter=True)
