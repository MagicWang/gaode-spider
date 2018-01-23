import { DbUtils } from "../utils/DbUtils";
import * as request from 'request';
import * as pinyin from 'pinyin';

const config = require('../config.json');

/**
 * 抓取高德行政区域
 */
export class DistrictSpider {
    constructor(parameters?) {

    }
    //高德key
    private keyIndex = 0;
    private gaodeKey = config.gaodeConfig.keys[this.keyIndex];//key每天的访问次数限制，达到上限时换下一个key
    private cities = [];//省、市、县三级行政区域
    private cityIndex = 0;
    private gaodeCity = null;
    public start() {
        this.initTable().then(() =>
            this.getAll().then(() =>
                this.gaodeDistrict()));
    }
    //判断表是否存在(不存在则创建)
    private initTable() {
        return new Promise((resolve, reject) => {
            DbUtils.instance.executeSql("drop table if exists district; create table district(id serial, citycode varchar(255) NOT NULL,adcode varchar(255) NOT NULL,pcode varchar(255),name varchar(255),pinyin varchar(255),center varchar(255),level varchar(255),polyline text,primary key(id))", null).then(() => {
                resolve();
            });
        });
    }
    //获取省、市、县三级行政区域
    private getAll() {
        return new Promise((resolve, reject) => {
            var url = 'http://restapi.amap.com/v3/config/district?key=&keywords=&subdistrict=3&extensions=base';
            url = url.replace('key=&', 'key=' + this.gaodeKey + '&');
            request(url, (error, response, body) => {
                body && (body = JSON.parse(body));
                if (body && body.status === "1") {
                    this.recursionDistrict(body.districts);
                    resolve();
                }
            });
        });
    }
    private recursionDistrict(districts, pcode?) {
        for (var i = 0; i < districts.length; i++) {
            var district = districts[i];
            district.pcode = pcode;
            if (district) {
                this.cities.push(district);
                district.districts && district.districts.length > 0 && this.recursionDistrict(district.districts, district.adcode);
            }
        }
    }
    //抓取高德行政区gaodeDistrict
    private gaodeDistrict() {
        this.gaodeCity = this.cities[this.cityIndex];
        var url = 'http://restapi.amap.com/v3/config/district?key=&keywords=&subdistrict=&extensions=all';
        url = url.replace('key=&', 'key=' + this.gaodeKey + '&').
            replace('keywords=&', 'keywords=' + this.gaodeCity.adcode + '&').
            replace('subdistrict=&', 'subdistrict=' + (this.gaodeCity.level === 'district' ? '1' : '0') + '&');
        request(url, (error, response, body) => {
            body && (body = JSON.parse(body));
            var infocode = body && body.infocode;
            if (infocode === "10003") {
                if (this.keyIndex < config.gaodeConfig.keys.length) {
                    console.log("key: " + this.gaodeKey + " 访问次数已达上限");
                    this.keyIndex++;
                    this.gaodeKey = config.gaodeConfig.keys[this.keyIndex];
                    this.gaodeDistrict();
                } else {
                    console.log("所有key访问次数已达上限;当前adcode:" + this.gaodeCity.adcode);
                }
            } else if (infocode === "10004") {
                console.log("key: " + this.gaodeKey + " 单位时间内访问过于频繁");
                setTimeout(() => {
                    this.gaodeDistrict();
                }, 100);
            } else {
                if (infocode === "10000") {
                    var district = body.districts && body.districts.length > 0 && body.districts[0];
                    if (district) {
                        district.pcode = this.gaodeCity.pcode;
                        this.insertDistrict(district);
                        var districts = district.districts;
                        if (districts && districts.length > 0) {
                            for (var i = 0; i < districts.length; i++) {
                                var street = districts[i];
                                street.pcode = district.adcode;
                                this.insertDistrict(street);
                            }
                        }
                    }
                } else {
                    console.log("key: " + this.gaodeKey + " infocode:" + infocode);
                }
                if (this.cityIndex < this.cities.length - 1) {
                    this.cityIndex++;
                    this.gaodeDistrict();
                }
            }
        });
    }
    private insertDistrict(district) {
        DbUtils.instance.executeSql('insert into district(citycode,adcode,pcode,name,pinyin,center,level,polyline) values($1,$2,$3,$4,$5,$6,$7,$8)', [district.citycode, district.adcode, district.pcode, district.name, pinyin(district.name).join(' '), district.center, district.level, district.polyline]);
    }
}