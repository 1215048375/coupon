run.sh和run.py的区别:
    参考scrapy启动： https://doc.scrapy.org/en/latest/topics/practices.html
    run.py 通过API方式启动spider, run.py启动多个spider时, 多个spider共享一个进程
    run.sh 通过脚本方式启动spider, run.sh启动多个spider时, 每个spider独享一个进程
    一次启动多个spider的情况下, 调试时建议使用run.py, 生产环境部署时建议使用run.sh

启动seller_spider(一个月爬取一次):
    1. 确认run.py中配置的是seller_spider
    2. 确认setting.py中日志文件配置为当前日期
    3. 确认seller.xxxxxxxx.json为当前日期, 且为空文件
    4. seller_spider只调用了淘宝接口采集数据, 故爬取速度非常快:
        1. ADSL切换时间5s, 不设置下载延时(实践证明会访问受限)
        2. ADSL切换时间3s, 设置下载延时0.5s(可以爬取)
        3. ADSL切换时间3s, 设置下载延时小于0.5s(TODO 待验证)
    5. 采集结束后, 确认日志中每个channel的商品数是否与阿里妈妈每个channel的商品数相等

启动coupon_spider(一天至少爬取一次):
    1. 确保./data/sellers.json文件被均分成多个小文件(执行命令: spilt -d -l 10000 sellers.json sellers.json_xx)
    2. 确认coupon_spider.py中SELLERS_FILE变量的值是否配置正确
    3. 确认run.py/run.sh中配置的是coupon_spider
    4. 确认setting.py中日志文件配置为coupon_spider
    5. coupon_spider只调用了接口采集数据, 故爬取速度非常快：
        1. ADSL切换时间3s, 设置下载延时0.1s(实践证明不可行)
        2. ADSL切换时间3s, 设置下载延时0.15s(实践证明可行)
        3. ADSL切换时间3s, 设置下载延时0.2s(实践证明可行)