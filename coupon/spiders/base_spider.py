# coding=utf-8
import scrapy
import re
from coupon.items import CouponItem


class BaseSpider(scrapy.Spider):
    name = "base_spider"    # spider名字
    custom_settings = {     # spider配置
        'ROBOTSTXT_OBEY': False,
        'DNS_TIMEOUT': 10,
        'DOWNLOAD_TIMEOUT': 60,
        # 'DOWNLOAD_DELAY': 0.05,
        'CONCURRENT_REQUESTS': 32,
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

        'MONGO_URI': 'mongodb://spider:spiderpasw@10.0.0.166:27017/tts_spider_deploy?authMechanism=SCRAM-SHA-1',
        'MONGO_DATABASE': 'tts_spider_deploy',
        'MONGO_COUPON_COLLECTION': 't_spider_product_coupon',
        'MONGO_SEQ_COLLECTION': 't_spider_product_seq'
    }
    seller_num = 0          # 抓取到优惠券的seller数量
    coupon_num = 0          # 抓取到的优惠券数量
    type = 0                # 0为隐藏券, 1为公开券
    wsite = 'taobao'        # 优惠券站点默认为淘宝

    taobao_headers = {      # 淘宝headers
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'shop.m.taobao.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BaseSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.logger.info('Spider closed: %s, and crawled %s sellers, total %s coupons' \
                         % (spider.name, self.seller_num, self.coupon_num))

    def parse_coupon(self, response):
        """ 解析领券页面,获取优惠券的详细信息
        :param response: 领券页面的响应
        """
        # 获取response中的数据
        meta = response.meta
        seller_index = meta['seller_index']
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
                'type': self.type,
                'price': price,
                'condition': condition,
                'mprice': mprice,
                'start': start,
                'end': end,
                'spids': spids,
                'flag': False
            }
            coupon_item = CouponItem()
            coupon_item['selid'] = meta['selid']
            coupon_item['nick'] = meta['nick']
            coupon_item['wsite'] = self.wsite
            coupon_item['coupons'] = [coupon]

            # 统计优惠券数量
            self.coupon_num += 1
            self.logger.info("%sth, selid: %s, nick: %s, acId: %s, spid: %s" % (seller_index, selid, nick, acId, spids[0]))

            # 抛出coupon_item
            yield coupon_item
        except Exception, e:
            # 解析优惠券信息异常
            self.logger.error("%sth, selid: %s, nick: %s, acId: %s, spid: %s, parse coupon fail(%s), %s" % (seller_index, selid, nick, acId, spids[0], e, response.url))
            pass

