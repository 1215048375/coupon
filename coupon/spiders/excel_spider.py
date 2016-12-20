# coding=utf-8
import scrapy
import xlrd
from coupon.spiders.base_spider import BaseSpider

# Excel文件路径
EXCEL_FILE = u'./data/精选优质商品清单(内含优惠券)-2016-12-20.xls'


class ExcelSpider(BaseSpider, scrapy.Spider):
    name = "excel_spider"   # spider名字
    type = 0                # 0为隐藏券, 1为公开券
    wsite = 'taobao'        # 优惠券站点为淘宝

    def start_requests(self):
        """重载父类的start_request方法, 建立会话"""
        # 打开excel文件
        data = xlrd.open_workbook(EXCEL_FILE)
        table = data.sheets()[0]
        nrows = table.nrows
        ncols = table.ncols

        # 获取excel文件中所需字段所在的列号
        spid_idx = -1  # A 0
        nick_idx = -1  # K 10
        selid_idx = -1  # L 11
        acid_idx = -1  # O 14
        for col in range(ncols):
            if table.cell(0, col).value == u'商品id':
                spid_idx = col
            if table.cell(0, col).value == u'卖家旺旺':
                nick_idx = col
            if table.cell(0, col).value == u'卖家id':
                selid_idx = col
            if table.cell(0, col).value == u'优惠券id':
                acid_idx = col

        # 对每个优惠券检测其是否店铺券
        for row in range(1, nrows):
            # 统计拿到优惠券的店铺数量
            self.seller_num += 1

            # 获取excel文件中优惠券id, 及其对应的卖家信息, 然后调用start_check函数检测
            nick = table.cell(row, nick_idx).value
            selid = table.cell(row, selid_idx).value
            spid = table.cell(row, spid_idx).value
            acIds = [table.cell(row, acid_idx).value]

            # 调用parse_coupon函数解析优惠券信息
            for acId in acIds:
                meta = {
                    'is_proxy': True,                   # ADSL代理
                    'seller_index': row,                # 当前优惠券所属的卖家索引
                    'selid': selid,                     # 当前优惠券所属的卖家id
                    'nick': nick,                       # 当前优惠券所属的卖家旺旺号
                    'acId': acId,                       # 当前优惠券id
                    'spids': [spid],                    # 当前优惠券适用的商品范围
                }
                url = 'http://shop.m.taobao.com/shop/coupon.htm?seller_id={0:s}&activity_id={1:s}'.format(selid, acId)
                yield scrapy.Request(url=url, meta=meta, headers=self.taobao_headers, callback=self.parse_coupon)