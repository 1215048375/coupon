# # coding=utf-8
# import scrapy
# import json
# import pykafka
# from coupon.spiders.base_spider import BaseSpider
#
#
# class KafkaSpider(BaseSpider, scrapy.Spider):
#     name = "kafka_spider"   # spider名字
#     type = 1                # 0为隐藏券, 1为公开券
#     wsite = 'taobao'        # 优惠券站点为淘宝
#
#     # 连接kafka, 创建consumer
#     kafka_client = pykafka.KafkaClient(zookeeper_hosts="10.0.0.225:2181, 10.0.0.226:2181, 10.0.0.227:2181/kafka")
#     kafka_topic = kafka_client.topics['ttk_coupon']
#     kafka_balanced_consumer = kafka_topic.get_balanced_consumer(consumer_group='ttk_coupon',
#                                                                 auto_offset_reset=pykafka.common.OffsetType.LATEST,
#                                                                 auto_commit_enable=True,
#                                                                 auto_commit_interval_ms=1000)
#
#     def start_requests(self):
#         """重载父类的start_request方法, 建立会话"""
#         for message in self.kafka_balanced_consumer:
#             try:
#                 # 解析出message中的优惠券信息
#                 message_ = json.loads(message.value.split('\t')[1])
#
#                 # 统计拿到优惠券的店铺数量
#                 self.seller_num += 1
#
#                 # 获取淘客助手返回的优惠券id, 及其对应的卖家信息, 然后调用check函数检测
#                 nick = message_['nick']
#                 selid = message_['selid']
#                 if (not nick) or (not selid):
#                     self.logger.error('%sth, partition_id: %s, nick/selid is empty. nick: %s, selid: %s' % (message.offset, message.partition_id, nick, selid))
#                     pass
#                 else:
#                     spid = -1
#                     acIds = [coupon['id'] for coupon in message_['priceVolumes']]
#
#                     # 调用parse_coupon函数解析优惠券信息
#                     for acId in acIds:
#                         meta = {
#                             'is_proxy': True,                   # ADSL代理
#                             'seller_index': message.offset,     # 当前优惠券所属的卖家索引
#                             'selid': selid,                     # 当前优惠券所属的卖家id
#                             'nick': nick,                       # 当前优惠券所属的卖家旺旺号
#                             'acId': acId,                       # 当前优惠券id
#                             'spids': [spid],                    # 当前优惠券适用的商品范围
#                         }
#                         url = 'http://shop.m.taobao.com/shop/coupon.htm?seller_id={0:s}&activity_id={1:s}'.format(selid, acId)
#                         yield scrapy.Request(url=url, meta=meta, headers=self.taobao_headers, callback=self.parse_coupon)
#             except Exception, e:
#                 # message不是json格式, 解析异常
#                 self.log('%sth, prase text to json error(%s), value: %s' % (message.offset, e, message.value))