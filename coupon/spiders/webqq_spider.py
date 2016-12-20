# coding=utf-8
import requests
import urllib
import urlparse
import json
import re
import sys
import logging
import datetime
import pymongo
from scrapy.selector import Selector
from coupon.items import CouponItem
from coupon.dynamicip import DynamicIP


# webqq cookie和请求headers
cookie = 'RK=3odaWaTXma; tvfe_boss_uuid=4e45382d4c870a86; ts_refer=www.google.co.jp/; ts_uid=3144663199; Hm_lvt_96d9d92b8a4aac83bc206b6c9fb2844a=1481172796,1481180940,1481186004,1481190525; Hm_lpvt_96d9d92b8a4aac83bc206b6c9fb2844a=1481190525; Hm_lvt_f5127c6793d40d199f68042b8a63e725=1481172796,1481180941,1481186004,1481190526; Hm_lpvt_f5127c6793d40d199f68042b8a63e725=1481190526; pgv_pvi=7820593152; pgv_si=s2164426752; pac_uid=1_282209415; ptcz=97579502bb84d03747445905a63f4dfa0da4117b9dd603ac425f4b5d7fe64965; pgv_info=ssid=s5628331708; pgv_pvid=1790142111; o_cookie=2675623447; ptisp=ctc; pt2gguin=o2675623447; uin=o2675623447; skey=@fIJL4ougD; p_uin=o2675623447; p_skey=**7gSiyZrcIm6U9kc*T4JMrBfv-T3FNSa3B5*pShwc4_; pt4_token=TWRqLj1BnQ7dWIEpwXi8nfShlxBacoDTXyKHC6snYyE_; ptwebqq=de01e3161b51982cfdf475c4aa7841ec12f89529c670ebb672da7287497d0b97'
ptwebqq = 'de01e3161b51982cfdf475c4aa7841ec12f89529c670ebb672da7287497d0b97'
clientid = 53999199
psessionid = '8368046764001d636f6e6e7365727665725f77656271714031302e3133332e34312e383400001ad00000066b026e040015808a206d0000000a406172314338344a69526d0000002859185d94e66218548d1ecb1a12513c86126b3afb97a3c2955b1070324790733ddb059ab166de6857'
webqq_message = 'r={0:s}'.format(urllib.quote(json.dumps({"ptwebqq": ptwebqq, "clientid": clientid, "psessionid": psessionid, "key": ""})))
webqq_headers = {
    'Host': 'd1.web2.qq.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Referer': 'https://d1.web2.qq.com/cfproxy.html?v=20151105001&callback=1',
    'Connection': 'keep-alive',
    'Cookie': cookie,
}

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

# mongodb配置
MONGO_URI = 'mongodb://spider:spiderpasw@10.0.0.166:27017/tts_spider_deploy?authMechanism=SCRAM-SHA-1'
MONGO_DATABASE = 'tts_spider_deploy'
MONGO_COUPON_COLLECTION = 't_spider_product_coupon'
MONGO_SEQ_COLLECTION = 't_spider_product_seq'


# Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s] %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S",  # datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='/home/yxx/software/pycharm-workspace/coupon/log/webqq_spider.' + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + '.log',
                    filemode='w')
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(filename)s:%(lineno)d %(levelname)s] %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)


class WebqqSpider(object):
    type = 0                # 0为隐藏券, 1为公开券
    wsite = 'taobao'        # 优惠券站点为淘宝

    # 连接mongodb
    client = pymongo.MongoClient(MONGO_URI)
    coupon_collection = client[MONGO_DATABASE][MONGO_COUPON_COLLECTION]
    seq_collection = client[MONGO_DATABASE][MONGO_SEQ_COLLECTION]
    # adsl
    dynamic_ip = DynamicIP('199.155.122.131:2181', '/adsl_proxy/lock')
    dynamic_ip.run()

    def start(self):
        """重载父类的start_request方法, 建立会话"""
        seller_index = 0
        while True:
            try:
                # 获取并解析qq群消息
                response = requests.post(url='https://d1.web2.qq.com/channel/poll2', headers=webqq_headers, data=webqq_message, timeout=30)
                message = self.parse_message(response=response)
                if len(message) != 3:
                    logger.info("craweled a new coupon failed: %s" % message)
                    continue
                logger.info("craweled a new coupon success. selid: %s, acId: %s, spid: %s" % (message['selid'], message['acId'], message['spid']))

                # 调用parse_coupon函数解析优惠券信息
                nick = self.get_nick(message['selid'], message['acId'], message['spid'])
                if nick:
                    logger.info("get nick for spid success. selid: %s, nick: %s, acId: %s, spid: %s" % (message['selid'], nick, message['acId'], message['spid']))
                    seller_index += 1
                    logger.error('0------------------------------------------------------------------------------------')
                    coupon_item = self.parse_coupon(seller_index, str(message['selid']), nick,  message['acId'], str(message['spid']))
                    logger.error('00------------------------------------------------------------------------------------')
                    if coupon_item:
                        self.update_database(coupon_item=coupon_item)
                        logger.error('000------------------------------------------------------------------------------------')
                    else:
                        logger.error('0000------------------------------------------------------------------------------------')
            except Exception, e:
                logger.error("raise some error(%s), to be continue" % (e))

    def parse_message(self, response):
        """从qq消息中解析出优惠券相关信息
        """
        message = dict()

        try:
            response = json.loads(response.text)
        except Exception, e:
            logger.error("parse webqq message to json error(%s), response: %s" % (e, response.text))
            return message

        try:
            # 正则匹配qq消息中的商品链接和优惠券链接
            if 'content' in response['result'][0]['value']:
                content = response['result'][0]['value']['content']
                content_ = ' '.join(content[1:])
                match = re.match(ur".*?(http.*?)\s+.*(http.*?)\s+.*", content_ + ' ')
                if not match:
                    # 群用户发送的普通消息
                    pass
                else:
                    # 确定商品链接和优惠券链接
                    coupon_url = match.group(1) if 'coupon' in match.group(1) else match.group(2)
                    product_url = match.group(1) if 'coupon' not in match.group(1) else match.group(2)

                    # 从优惠券链接中解析出卖家id和优惠券id
                    coupon = urlparse.parse_qsl(urlparse.urlparse(coupon_url).query)
                    if 'seller' in coupon[0][0]:
                        message['selid'] = coupon[0][1]
                        message['acId'] = coupon[1][1]
                    else:
                        message['selid'] = coupon[1][1]
                        message['acId'] = coupon[0][1]

                    # 从商品链接中解析出商品id
                    if 's.click' in product_url:
                        proxies = dict()
                        proxy = self.dynamic_ip.get_proxy()
                        if proxy:
                            proxies['http'] = "http://{0:s}:3128".format(proxy)
                            proxies['https'] = "http://{0:s}:3128".format(proxy)
                        product_url = requests.get(product_url, proxies=proxies, verify=False).url
                        product_url = requests.get(urllib.unquote(product_url.split('tu=')[1]), proxies=proxies, headers={'Referer': product_url}, verify=False).url
                        product = urlparse.parse_qsl(urlparse.urlparse(product_url).query)
                    else:
                        product = urlparse.parse_qsl(urlparse.urlparse(product_url).query)
                    for elem in product:
                        if 'id' == elem[0]:
                            message['spid'] = elem[1]
                            break
        except Exception, e:
            logger.error('parse coupon info from webqq message error(%s), response: %s' % (e, content[1]))
        return message

    def get_nick(self, selid, acId, spid):
        # 商品对应的卖家昵称
        nick = None

        # 从阿里妈妈获取商品信息
        product_url = 'http://pub.alimama.com/items/search.json?q=http%3A%2F%2Fitem.taobao.com%2Fitem.htm%3Fid%3D{0:s}&perPageSize=50'.format(spid)
        proxies = dict()
        proxy = self.dynamic_ip.get_proxy()
        if proxy:
            proxies['http'] = "http://{0:s}:3128".format(proxy)
            proxies['https'] = "http://{0:s}:3128".format(proxy)
        try:
            response = requests.get(product_url, proxies=proxies, verify=False, timeout=3)
        except Exception, e:
            logger.error('get nick for spid error(%s), selid: %s, acId: %s, spid: %s. %s' % (e, selid, acId, spid, product_url))
            return nick
        data = json.loads(response.text)

        # 返回结果中没有商品信息
        if not data['data']['head']['docsfound']:
            errmsg = data['data']['head']['errmsg']
            # 确实没有满足条件的doc返回
            if "no keyword search failed" in errmsg:
                logger.error('get nick for spid failed, no docs found (%s), selid: %s, acId: %s, spid: %s' % (errmsg, selid, acId, spid))
                pass
            # 有满足条件的doc返回, 但由于超时等原因导致无返回结果
            else:
                logger.error('get nick for spid failed, no docs found (%s), selid: %s, acId: %s, spid: %s' % (errmsg, selid, acId, spid))
                pass
        # 返回结果中有商品信息
        else:
            nick = data['data']['pageList'][0]['nick']
        return nick

    def parse_coupon(self, seller_index, selid, nick, acId, spid):
        """ 解析领券页面,获取优惠券的详细信息
        :param response: 领券页面的响应
        """
        # 获取优惠券页面
        logger.error('*------------------------------------------------------------------------------------')
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
                logger.error('**------------------------------------------------------------------------------------')
                break
            except:
                logger.error('***------------------------------------------------------------------------------------')
                pass

        # 从response返回的优惠券页面中解析优惠券信息
        logger.error('****------------------------------------------------------------------------------------')
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

            # 为优惠券存入数据库时的必需字段赋值
            coupon = {
                'acId': acId,
                'type': self.type,
                'price': price,
                'condition': condition,
                'mprice': mprice,
                'start': start,
                'end': end,
                'spids': [spid],
                'flag': False
            }
            coupon_item = CouponItem()
            coupon_item['selid'] = selid
            coupon_item['nick'] = nick
            coupon_item['wsite'] = self.wsite
            coupon_item['coupons'] = [coupon]
            return coupon_item
        except Exception, e:
            # 解析优惠券信息异常
            logger.error("%sth, selid: %s, nick: %s, acId: %s, parse coupon fail(%s), %s" % (seller_index, selid, nick, acId, e, response.url))
            return None

    def update_database(self, coupon_item):
        # 获取(爬取到的)优惠券coupon_item对应的店铺在数据库中的数据
        logger.error('1------------------------------------------------------------------------------------')
        selid = coupon_item['selid']
        logger.error('2------------------------------------------------------------------------------------')
        acId = coupon_item['coupons'][0]['acId']
        logger.error('3------------------------------------------------------------------------------------')
        data = self.coupon_collection.find_one(filter={'selid': selid}, max_time_ms=300)
        logger.error('4------------------------------------------------------------------------------------')

        # 爬取到的优惠券对应的店铺已经存在于数据库中, 则update优惠券
        if data:
            # 爬取到的优惠券已经存在于数据库中, 则更新type和spids字段
            changed = False
            acIds = [coupon['acId'] for coupon in data['coupons']]
            logger.error('5------------------------------------------------------------------------------------')
            if acId in acIds:
                # 爬取到优惠券在所有优惠券中的索引
                index = acIds.index(acId)
                logger.error('6------------------------------------------------------------------------------------')
                # 爬取到的优惠券的公开/隐藏属性(newcome_type)和数据库中已存在的公开/隐藏属性(existed_type)
                newcome_type = coupon_item['coupons'][0]['type']
                logger.error('7------------------------------------------------------------------------------------')
                existed_type = data['coupons'][index]['type']
                logger.error('8------------------------------------------------------------------------------------')

                # 爬取到的优惠券的适用范围(newcome_spids)和数据库中已存在的适用范围(existed_spids)
                newcome_spids = coupon_item['coupons'][0]['spids']
                logger.error('9------------------------------------------------------------------------------------')
                existed_spids = data['coupons'][index]['spids']
                logger.error('10------------------------------------------------------------------------------------')

                     # 更新优惠券的公开/隐藏属性
                if (newcome_type == 1) and (existed_type == 0):
                    data['coupons'][index]['type'] = newcome_type
                    logger.error('11------------------------------------------------------------------------------------')
                    changed = True

                # 更新优惠券的单品/通用属性
                if newcome_spids != ["-1"]:
                    if existed_spids != ["-1"]:
                        union_spids = list(set(existed_spids) | set(newcome_spids))
                        logger.error('12------------------------------------------------------------------------------------')
                        data['coupons'][index]['spids'] = union_spids
                        logger.error('13------------------------------------------------------------------------------------')
                        changed = True if len(union_spids) != len(existed_spids) else False
                    else:
                        data['coupons'][index]['spids'] = newcome_spids
                        logger.error('14------------------------------------------------------------------------------------')
                        changed = True
            # 爬取到的优惠券不在数据库中, 则添加新优惠券
            else:
                data['coupons'] += coupon_item['coupons']
                logger.error('15------------------------------------------------------------------------------------')
                changed = True

            # update
            if changed:
                logger.info("update the new coupon. selid: %s, nick: %s, acId: %s, spid: %s" %
                            (selid, coupon_item['nick'], coupon_item['coupons'][0]['acId'], coupon_item['coupons'][0]['spids'][0]))
                self.coupon_collection.update({'selid': selid}, {'$set': {'coupons': data['coupons'], 'mtime': datetime.datetime.now()}})
                logger.error('16------------------------------------------------------------------------------------')
            else:
                logger.info("not update the new coupon. selid: %s, nick: %s, acId: %s, spid: %s" %
                            (selid, coupon_item['nick'], coupon_item['coupons'][0]['acId'], coupon_item['coupons'][0]['spids'][0]))

        # 店铺的优惠券不存在, 则先获取_id, 然后插入该店铺的优惠券
        else:
            logger.info("insert the new coupon. selid: %s, nick: %s, acId: %s, spid: %s" %
                            (selid, coupon_item['nick'], coupon_item['coupons'][0]['acId'], coupon_item['coupons'][0]['spids'][0]))
            coupon_item['_id'] = self.seq_collection.find_and_modify({'type': 'coupon'}, {'$inc': {'seq': 1}})['seq']
            logger.error('17------------------------------------------------------------------------------------')
            coupon_item['ctime'] = coupon_item['mtime'] = datetime.datetime.now()
            logger.error('18------------------------------------------------------------------------------------')
            self.coupon_collection.insert_one(dict(coupon_item))
            logger.error('19------------------------------------------------------------------------------------')

if __name__ == '__main__':
    webqq_spider = WebqqSpider()
    webqq_spider.start()