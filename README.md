# gaode-spider
高德POI、路网(不是很全)、行政区域爬虫
在config.json中配置postgresql数据库连接信息，将会自动创建表district(行政区域表)、poi(poi表)、road(道路表);
已有10个高德key,均为企业开发者key,日访问上限40万次;
city可不填，将会循环获取所有城市的poi,或者填一个citycode,只获取此城市的poi;
extent为需要抓取的路网的范围(高德api限制每次请求矩形的对角线不超过10km)