import { DbUtils } from "../utils/DbUtils";
import * as request from 'request';

const config = require('../config.json');
const xlsx = require("node-xlsx");

/**
 * 抓取高德poi点
 */
export class PoiSpider {
    constructor(parameters?) {

    }
    //高德key
    private keyIndex = 0;
    private cityIndex = 0;
    private cities = [];
    private gaodeKey = config.gaodeConfig.keys[this.keyIndex];//key每天的访问次数限制，达到上限时换下一个key
    private gaodeCity = config.gaodeConfig.city;//config中city为空时抓取全国poi,否则只抓取指定城市的poi
    //高德poi类型
    private types = [];
    private typeIndex = 0;
    private workSheetsFromFile = xlsx.parse(`${__dirname}/高德地图API POI分类编码表.xlsx`);
    //分页请求相关
    private page = 1;
    private pageSize = 50;//强烈建议不超过25，若超过25可能造成访问报错(经测试，最大50条)
    private errorCount = 0;//同一个请求错误三次之后跳到下一个类型

    public init() {
        if (!this.workSheetsFromFile) {
            console.log("Poi分类编码表不存在或者解析错误");
            return;
        }
        for (var i = 0; i < this.workSheetsFromFile.length; i++) {
            var sheet = this.workSheetsFromFile[i];
            if (sheet && sheet.name === 'POI分类与编码（中英文）') {
                for (var j = 1; j < sheet.data.length; j++) {
                    var data = sheet.data[j];
                    data[1] && this.types.push(data[1] + '');
                }
                break;
            }
        }
        this.initTable();
    }

    //判断表是否存在(不存在则创建)
    private initTable() {
        if (!this.gaodeCity) {
            this.initDistrictTable().then(() => {
                this.initPoiTable().then(() => this.gaodePoi());
            });
        } else {
            this.initPoiTable().then(() => this.gaodePoi());
        }
    }
    private initDistrictTable() {
        return new Promise((resolve, reject) => {
            DbUtils.instance.executeSql("drop table if exists district; create table district(id serial, citycode varchar(255) NOT NULL,adcode varchar(255) NOT NULL,name varchar(255),center varchar(255),level varchar(255),polyline text,primary key(id))", null).then(() => {
                this.gaodeDistrict();
                DbUtils.instance.executeSql("select * from district where level='city'", null).then(result => {
                    for (var i = 0; i < result.rows.length; i++) {
                        var row = result.rows[i];
                        this.cities.push(row.citycode);
                    }
                    this.gaodeCity = this.cities[this.cityIndex];
                    resolve();
                });
            });
        });
    }
    private initPoiTable() {
        return new Promise((resolve, reject) => {
            DbUtils.instance.executeSql("select count(*) from pg_class where relname = 'poi'", null).then(result => {
                if (!result || !result.rows[0] || result.rows[0].count === '0') {
                    DbUtils.instance.executeSql("create table poi(id serial,gid varchar(255) NOT NULL,name varchar(255),type varchar(255),typecode varchar(255),biz_type varchar(255),address varchar(255),location varchar(255),tel varchar(255),distance varchar(255),biz_ext varchar(255),pname varchar(255),cityname varchar(255),adname varchar(255),importance varchar(255),shopid varchar(255),shopinfo varchar(255),poiweight varchar(255),primary key(id))").then(() => {
                        resolve();
                    });
                } else {
                    resolve();
                }
            });
        });
    }

    //抓取高德行政区gaodeDistrict
    private gaodeDistrict() {
        var url = 'http://restapi.amap.com/v3/config/district?key=&keywords=&subdistrict=3&extensions=base';
        url = url.replace('key=&', 'key=' + this.gaodeKey + '&');
        request(url, (error, response, body) => {
            body && (body = JSON.parse(body));
            if (body && body.status === "1") {
                this.recursionDistrict(body.districts);
                console.log("execute gaodeDistrict end");
            }
        });
    }
    private recursionDistrict(districts) {
        for (var i = 0; i < districts.length; i++) {
            var district = districts[i];
            DbUtils.instance.executeSql('insert into district(citycode,adcode,name,center,level) values($1,$2,$3,$4,$5)', [district.citycode, district.adcode, district.name, district.center, district.level]);
            district && district.districts && district.districts.length > 0 && this.recursionDistrict(district.districts);
        }
    }

    //抓取高德poi
    private gaodePoi() {
        var url = 'http://restapi.amap.com/v3/place/text?key=&keywords=&types=&city=&citylimit=true&children=0&offset=&page=&extensions=base';
        url = url.replace('key=&', 'key=' + this.gaodeKey + '&').replace('types=&', 'types=' + this.types[this.typeIndex] + '&').replace('city=&', 'city=' + this.gaodeCity + '&').replace('offset=&', 'offset=' + this.pageSize + '&').replace('page=&', 'page=' + this.page + '&');
        request(url, (error, response, body) => {
            body && (body = JSON.parse(body));
            var infocode = body && body.infocode;
            if (infocode === "10003") {
                if (this.keyIndex < config.gaodeConfig.keys.length) {
                    console.log("key: " + this.gaodeKey + " 访问次数已达上限");
                    this.keyIndex++;
                    this.gaodeKey = config.gaodeConfig.keys[this.keyIndex];
                    this.gaodePoi();
                } else {
                    console.log("所有key访问次数已达上限;当前citycode:" + this.gaodeCity + ";当前poi类型:" + this.types[this.typeIndex]);
                }
            } else if (infocode === "10004") {
                console.log("key: " + this.gaodeKey + " 单位时间内访问过于频繁");
                setTimeout(() => {
                    this.gaodePoi();
                }, 100);
            } else {
                if (infocode === "10000") {
                    this.insertPoi(body.pois);
                } else {
                    error && this.errorCount++;
                    console.log("key: " + this.gaodeKey + " " + (error || body && body.info));
                }
                // count属性不准
                if (body && body.pois && body.pois.length >= this.pageSize) {
                    this.errorCount = 0;
                    this.page++;
                } else if (!error || this.errorCount > 3) {
                    this.errorCount = 0;
                    this.typeIndex++;
                    this.page = 1;
                }
                if (this.typeIndex < this.types.length) {
                    this.gaodePoi();
                } else if (this.cities && this.cities.length > 0 && this.cityIndex < this.cities.length) {
                    this.cityIndex++;
                    this.gaodeCity = this.cities[this.cityIndex];
                    this.typeIndex = 0;
                    this.page = 1;
                    this.gaodePoi();
                } else {
                    console.log("execute gaodePoi end");
                    this.distinct();
                }
            }
        });
    }
    private insertPoi(pois) {
        for (var i = 0; i < pois.length; i++) {
            var poi = pois[i];
            DbUtils.instance.executeSql('insert into poi(gid,name,type,typecode,biz_type,address,location,tel,distance,biz_ext,pname,cityname,adname,importance,shopid,shopinfo,poiweight) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)', [poi.id, poi.name, poi.type, poi.typecode, poi.biz_type, poi.address, poi.location, poi.tel, poi.distance, poi.biz_ext, poi.pname, poi.cityname, poi.adname, poi.importance, poi.shopid, poi.shopinfo, poi.poiweight]);
        }
    }
    private distinct() {
        DbUtils.instance.executeSql("delete from poi where ctid not in (select min(ctid) from poi group by gid)").then(() => {
            console.log("execute distinct end");
        })
    }
}