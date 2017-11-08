import * as pg from 'pg';
const config = require('../config.json');

export class DbUtils {
    private pool: pg.Pool = new pg.Pool(config.pgConfig);
    public static instance: DbUtils = new DbUtils();
    constructor() {
        this.pool.on("error", (err, client) => {
            console.log("数据库连接出错 --> ", err)
        });
    }
    //执行sql语句
    executeSql(queryText: string, values?: any[]): Promise<pg.QueryResult> {
        return new Promise((resolve, reject) => {
            this.pool.connect().then(client => {
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
}