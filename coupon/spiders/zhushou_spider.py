# -*- coding: utf-8 -*-
"""
该程序从淘客助手的接口采集优惠券id.
"""
import random
import scrapy
import json
import time
from coupon.spiders.base_spider import BaseSpider

# 卖家信息文件
SELLERS_FILE = './data/sellers.mongodb.json.15'


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


class ZhuShouSpider(BaseSpider, scrapy.Spider):
    name = "zhushou_spider"  # spider名字
    type = 0                 # 0为隐藏券, 1为公开券
    wsite = 'taobao'         # 优惠券站点为淘宝
    session = ''             # 淘客助手会话
    zhushou_headers = {      # 淘客助手headers
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'zhushou3.taokezhushou.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
    }
    sellers = get_sellers()  # 从文件中载入所有seller信息

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
        self.zhushou_headers['Cookie'] = cookie

        # 用获取到cookie, 获取第一个店铺的广告/优惠券数据
        seller_index = 0
        seller = self.sellers[seller_index]
        url = 'http://zhushou3.taokezhushou.com/api/v1/getdata?itemid={0:d}&version=3.5.1'.format(random.randint(1000000, 99999999))
        yield scrapy.Request(url=url, meta={'seller_index': seller_index}, headers=self.zhushou_headers, callback=self.parse_ad)
        url = 'http://zhushou3.taokezhushou.com/api/v1/coupons_base/{0:s}?item_id={1:d}'.format(seller['sellerId'], random.randint(1000000, 99999999))
        yield scrapy.Request(url=url, meta={'seller_index': seller_index}, headers=self.zhushou_headers, callback=self.parse_acIds)

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
        self.zhushou_headers['Cookie'] = cookie

        # 处理优惠券数据
        try:
            # 解析从淘客助手的返回的优惠券数据
            data = json.loads(response.body)
            if data['data']:
                if data['data'][0] is not None:
                    # 统计拿到优惠券的店铺数量
                    self.seller_num += 1

                    # 获取淘客助手返回的优惠券id, 及其对应的卖家信息, 然后调用check函数检测
                    nick = seller['nick']
                    selid = seller['sellerId']
                    spid = '-1'
                    acIds = [item['activity_id'] for item in data['data']]

                    # 调用parse_coupon函数解析优惠券信息
                    for acId in acIds:
                        meta = {
                            'is_proxy': True,                   # ADSL代理
                            'seller_index': seller_index,       # 当前优惠券所属的卖家索引
                            'selid': selid,                     # 当前优惠券所属的卖家id
                            'nick': nick,                       # 当前优惠券所属的卖家旺旺号
                            'acId': acId,                       # 当前优惠券id
                            'spids': [spid],                        # 当前优惠券适用的商品范围
                        }
                        url = 'http://shop.m.taobao.com/shop/coupon.htm?seller_id={0:s}&activity_id={1:s}'.format(selid, acId)
                        yield scrapy.Request(url=url, meta=meta, headers=self.taobao_headers, callback=self.parse_coupon)
                else:
                    # 淘客助手存在该店铺的优惠券数据, 但其反作弊手段导致无数据返回
                    self.logger.error('%sth sellerId: %s, nick: %s, activities exist, but return nothing. %s' % (seller_index, seller['sellerId'], seller['nick'], response.url))
            else:
                # 淘客助手不存在该店铺的优惠券数据
                self.logger.error('%sth sellerId: %s, nick: %s, activities not exist. %s' % (seller_index, seller['sellerId'], seller['nick'], response.url))
        except Exception, e:
            # 淘客助手返回的数据不能解析成json格式, 一般是因为其防攻击策略, 则休眠一段时间重新请求受限的请求
            self.logger.error('restricted access. %sth sellerId: %s, nick: %s, session: %s, %s' % (seller_index, seller['sellerId'], seller['nick'], self.session, response.url))
            time.sleep(1)
            seller_index -= 1

        # 获取下一个店铺的广告/优惠券数据
        seller_index += 1
        if seller_index < len(self.sellers):
            seller = self.sellers[seller_index]
            url = 'http://zhushou3.taokezhushou.com/api/v1/getdata?itemid={0:d}&version=3.5.1'.format(random.randint(1000000, 99999999))
            yield scrapy.Request(url=url, meta={'seller_index': seller_index}, headers=self.zhushou_headers, callback=self.parse_ad, dont_filter=True)
            url = 'http://zhushou3.taokezhushou.com/api/v1/coupons_base/{0:s}?item_id={1:d}'.format(seller['sellerId'], random.randint(1000000, 99999999))
            yield scrapy.Request(url=url, meta={'seller_index': seller_index}, headers=self.zhushou_headers, callback=self.parse_acIds, dont_filter=True)
        else:
            pass