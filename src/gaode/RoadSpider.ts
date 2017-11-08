import { DbUtils } from "../utils/DbUtils";
import * as request from 'request';
const config = require('../config.json');
/**
 * 抓取高德路网
 */
export class RoadSpider {
    constructor(parameters?) {

    }
    //高德key
    private keyIndex = 0;
    private gaodeKey = config.gaodeConfig.keys[this.keyIndex];//key每天的访问次数限制，达到上限时换下一个key

    private offsetX = 0.052;//中国5km大约代表经度数值
    private offsetY = 0.045;//中国5km大约代表纬度数值
    private grids = [];
    private gridIndex = 0;
    private levels = [1, 2, 3, 4, 5, 6];
    private levelIndex = 0;
    private errorCount = 0;//同一个请求错误三次之后跳到下一个类型

    public init() {
        this.initGrid();
        this.initRoadTable().then(() => this.gaodeRoad());
    }
    private initGrid() {
        this.grids = [];
        var extent = config && config.gaodeConfig && config.gaodeConfig.extent;
        if (extent && extent.length >= 4) {
            var start, end;
            var lenX = Math.ceil((extent[2] - extent[0]) / this.offsetX);
            var lenY = Math.ceil((extent[3] - extent[1]) / this.offsetY);
            for (var i = 0; i < lenX - 1; i++) {
                for (var j = 0; j < lenY - 1; j++) {
                    start = [(extent[0] + i * this.offsetX).toFixed(6), (extent[1] + j * this.offsetY).toFixed(6)];
                    end = [(extent[0] + (i + 1) * this.offsetX).toFixed(6), (extent[1] + (j + 1) * this.offsetY).toFixed(6)];
                    this.grids.push(start.join() + ";" + end.join());
                }
            }
        }
    }
    //判断路网表是否存在(不存在则创建)
    private initRoadTable() {
        return new Promise((resolve, reject) => {
            DbUtils.instance.executeSql("select count(*) from pg_class where relname = 'road'", null).then(result => {
                if (!result || !result.rows[0] || result.rows[0].count === '0') {
                    DbUtils.instance.executeSql("create table road(id serial,name varchar(255) NULL,level int2,polyline text,status varchar(255),speed varchar(255),angle varchar(255),direction varchar(255),lcodes varchar(255),primary key(id))").then(() => {
                        resolve();
                    });
                } else {
                    resolve();
                }
            });
        });
    }
    //循环获取高德道路数据
    private gaodeRoad() {
        var url = 'http://restapi.amap.com/v3/traffic/status/rectangle?key=&rectangle=&level=&extensions=all';
        url = url.replace('key=&', 'key=' + this.gaodeKey + '&').replace('rectangle=&', 'rectangle=' + this.grids[this.gridIndex] + '&').replace('level=&', 'level=' + this.levels[this.levelIndex] + '&');
        request(url, (error, response, body) => {
            body && (body = JSON.parse(body));
            var infocode = body && body.infocode;
            if (infocode === "10003") {
                if (this.keyIndex < config.gaodeConfig.keys.length) {
                    console.log("key: " + this.gaodeKey + " 访问次数已达上限");
                    this.keyIndex++;
                    this.gaodeKey = config.gaodeConfig.keys[this.keyIndex];
                    this.gaodeRoad();
                } else {
                    console.log("所有key访问次数已达上限;rectangle:" + this.grids[this.gridIndex] + ";当前level:" + this.levels[this.levelIndex]);
                }
            } else if (infocode === "10004") {
                console.log("key: " + this.gaodeKey + " 单位时间内访问过于频繁");
                setTimeout(() => {
                    this.gaodeRoad();
                }, 100);
            } else {
                if (infocode === "10000") {
                    this.errorCount = 0;
                    this.insertRoad(body.trafficinfo.roads);
                    this.levelIndex++;
                } else {
                    if (++this.errorCount >= 3) {
                        this.levelIndex++;
                    }
                    console.log("key: " + this.gaodeKey + " " + (body && body.info));
                }
                if (this.levelIndex >= this.levels.length) {
                    this.gridIndex++;
                    this.levelIndex = 0;
                }
                if (this.gridIndex < this.grids.length) {
                    this.gaodeRoad();
                } else {
                    console.log("execute gaodeRoad end");
                    this.distinct();
                }
            }
        });
    }
    //添加道路数据
    private insertRoad(roads) {
        for (var i = 0; i < roads.length; i++) {
            var road = roads[i];
            DbUtils.instance.executeSql('insert into road(name,level,polyline,status,speed,angle,direction,lcodes) values($1,$2,$3,$4,$5,$6,$7,$8)', [road.name, this.levels[this.levelIndex], road.polyline, road.status, road.speed, road.angle, road.direction, road.lcodes]);
        }
    }
    //添加几何字段，并去除重复数据
    private distinct() {
        DbUtils.instance.executeSql("SELECT addgeometrycolumn('road', 'geom', 4326, 'LINESTRING',2)").then(() => {
            DbUtils.instance.executeSql("update road set geom=ST_LineFromText('LINESTRING('||replace(replace(polyline,',',' '),';',',')||')',4326)").then(() => {
                DbUtils.instance.executeSql("delete from road where ctid not in (select min(ctid) from road group by geom, level)").then(() => {
                    console.log("execute distinct end");
                })
            })
        })
    }
}