#!/usr/bin/env bash

# Python
export PATH="/home/murphy/anaconda2-4.0.0/bin:$PATH"

# 启动coupon_spider
nohup scrapy crawl coupon_spider 1>./log/nohup.$(date +%Y%m%d-%H%M%S).log 2>&1 &