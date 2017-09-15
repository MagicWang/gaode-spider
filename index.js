var pg = require('pg');
var request = require('request');
var fs = require('file-system');
var xlsx = require("node-xlsx");

var config = JSON.parse(fs.readFileSync(__dirname + "/config.json", 'utf8'));
var pool = new pg.Pool(config.pgConfig);
pool.on("error", function (err, client) {
    console.log("数据库连接出错 --> ", err)
});

//高德key
var keyIndex = 0;
var cityIndex = 0;
var cities = [];
var gaodeKey = config.gaodeConfig.keys[keyIndex];//key每天的访问次数限制，达到上限时换下一个key
var gaodeCity = config.gaodeConfig.city;//config中city为空时抓取全国poi,否则只抓取指定城市的poi
//高德poi类型
var types = [];
var typeIndex = 0;
var workSheetsFromFile = xlsx.parse(`${__dirname}/高德地图API POI分类编码表.xlsx`);
if (!workSheetsFromFile) {
    console.log("Poi分类编码表不存在或者解析错误");
    return;
}
for (var i = 0; i < workSheetsFromFile.length; i++) {
    var sheet = workSheetsFromFile[i];
    if (sheet && sheet.name === 'POI分类与编码（中英文）') {
        for (var j = 1; j < sheet.data.length; j++) {
            var data = sheet.data[j];
            data[1] && types.push(data[1] + '');
        }
        break;
    }
}

var page = 1;
var pageSize = 50;//强烈建议不超过25，若超过25可能造成访问报错(经测试，最大50条)
var errorCount = 0;//同一个请求错误三次之后跳到下一个类型

//判断表是否存在(不存在则创建)
function initTable() {
    if (!gaodeCity) {
        initDistrictTable().then(() => {
            initPoiTable();
        });
    } else {
        initPoiTable();
    }
}
function initDistrictTable() {
    return new Promise(function (resolve, reject) {
        executeSql("drop table if exists district; create table district(id serial, citycode varchar(255) NOT NULL,adcode varchar(255) NOT NULL,name varchar(255),center varchar(255),level varchar(255),primary key(id))", null).then(() => {
            gaodeDistrict().then(() => {
                executeSql("select * from district where level='city'", null).then(result => {
                    for (var i = 0; i < result.rows.length; i++) {
                        var row = result.rows[i];
                        cities.push(row.citycode);
                    }
                    gaodeCity = cities[cityIndex];
                    resolve();
                });
            });
        });
    });
}
function initPoiTable() {
    executeSql("select count(*) from pg_class where relname = 'poi'", null).then(result => {
        if (!result || !result.rows[0] || result.rows[0].count === '0') {
            executeSql("create table poi(id serial,gid varchar(255) NOT NULL,name varchar(255),type varchar(255),typecode varchar(255),biz_type varchar(255),address varchar(255),location varchar(255),tel varchar(255),distance varchar(255),biz_ext varchar(255),pname varchar(255),cityname varchar(255),adname varchar(255),importance varchar(255),shopid varchar(255),shopinfo varchar(255),poiweight varchar(255),primary key(id))").then(() => {
                gaodePoi();
            });
        } else {
            gaodePoi();
        }
    });
}
//执行sql语句
function executeSql(queryText, values) {
    return new Promise(function (resolve, reject) {
        pool.connect().then(client => {
            client.query(queryText, values).then(result => {
                client.release();
                resolve(result);
            }).catch(err => {
                client.release();
                console.error('执行出错', err)
            })
        });
    });
}

//抓取高德行政区gaodeDistrict
function gaodeDistrict() {
    return new Promise(function (resolve, reject) {
        var url = 'http://restapi.amap.com/v3/config/district?key=&keywords=&subdistrict=3&extensions=base';
        url = url.replace('key=&', 'key=' + gaodeKey + '&');
        request(url, function (error, response, body) {
            body && (body = JSON.parse(body));
            if (body && body.status === "1") {
                recursionDistrict(body.districts);
                console.log("execute gaodeDistrict end");
                resolve();
            }
        });
    });
}
function recursionDistrict(districts) {
    for (var i = 0; i < districts.length; i++) {
        var district = districts[i];
        executeSql('insert into district(citycode,adcode,name,center,level) values($1,$2,$3,$4,$5)', [district.citycode, district.adcode, district.name, district.center, district.level]);
        district && district.districts && district.districts.length > 0 && recursionDistrict(district.districts);
    }
}

//抓取高德poi
function gaodePoi() {
    return new Promise(function (resolve, reject) {
        var url = 'http://restapi.amap.com/v3/place/text?key=&keywords=&types=&city=&citylimit=true&children=0&offset=&page=&extensions=base'
        url = url.replace('key=&', 'key=' + gaodeKey + '&').replace('types=&', 'types=' + types[typeIndex] + '&').replace('city=&', 'city=' + gaodeCity + '&').replace('offset=&', 'offset=' + pageSize + '&').replace('page=&', 'page=' + page + '&');
        request(url, function (error, response, body) {
            body && (body = JSON.parse(body));
            var infocode = body && body.infocode;
            if (infocode === "10003") {
                if (keyIndex < config.gaodeConfig.keys.length) {
                    console.log("key: " + gaodeKey + " 访问次数已达上限");
                    keyIndex++;
                    gaodeKey = config.gaodeConfig.keys[keyIndex];
                    gaodePoi();
                } else {
                    console.log("所有key访问次数已达上限;当前citycode:" + gaodeCity + ";当前poi类型:" + types[typeIndex]);
                }
            } else if (infocode === "10004") {
                console.log("key: " + gaodeKey + " 单位时间内访问过于频繁");
                setTimeout(function () {
                    gaodePoi();
                }, 100);
            } else {
                if (infocode === "10000") {
                    recursionPoi(body.pois);
                } else {
                    error && errorCount++;
                    console.log("key: " + gaodeKey + " " + (error || body && body.info));
                }
                // count属性不准
                if (body && body.pois && body.pois.length >= pageSize) {
                    errorCount = 0;
                    page++;
                } else if (!error || errorCount > 3) {
                    errorCount = 0;
                    typeIndex++;
                    page = 1;
                }
                if (typeIndex < types.length) {
                    gaodePoi();
                } else if (cities && cities.length > 0 && cityIndex < cities.length) {
                    cityIndex++;
                    gaodeCity = cities[cityIndex];
                    typeIndex = 0;
                    page = 1;
                    gaodePoi();
                } else {
                    console.log("execute gaodePoi end");
                    resolve();
                }
            }
        });
    });
}
function recursionPoi(pois) {
    for (var i = 0; i < pois.length; i++) {
        var poi = pois[i];
        executeSql('insert into poi(gid,name,type,typecode,biz_type,address,location,tel,distance,biz_ext,pname,cityname,adname,importance,shopid,shopinfo,poiweight) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)', [poi.id, poi.name, poi.type, poi.typecode, poi.biz_type, poi.address, poi.location, poi.tel, poi.distance, poi.biz_ext, poi.pname, poi.cityname, poi.adname, poi.importance, poi.shopid, poi.shopinfo, poi.poiweight]);
    }
}

initTable();
