TODO - 161性能存在问题，程序需部署到其他机器上

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
    1. 确保./data/sellers.xxxxxxxx.json文件被均分成多个小文件(执行命令: spilt -d -l 10000 sellers.********.json sellers.********.json_xx)
    1. 确认run.py中配置的是coupon_spider_*, 且配置的coupon_spider_*总数等于步骤1中小文件总数
    2. 确认setting.py中日志文件配置为当前日期
    3. coupon_spider只调用了接口采集数据, 故爬取速度非常快：
        1. ADSL切换时间3s, 设置下载延时0.15s(实践证明不可行)
        1. ADSL切换时间3s, 设置下载延时0.2s(实践证明可行)
        2. ADSL切换时间3s, 设置下载延时小于0.1s(实践证明可行)