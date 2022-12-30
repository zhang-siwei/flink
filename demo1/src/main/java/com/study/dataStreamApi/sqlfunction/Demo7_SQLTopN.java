package com.study.dataStreamApi.sqlfunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created by Smexy on 2022/12/23
 */
public class Demo7_SQLTopN
{
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //建表(生成et,和水印)，映射数据
        String  createTableSQL = " CREATE TABLE t1( userId bigint, itemId bigint , cId int  , behavior string, ts bigint , " +
            "                              et  AS TO_TIMESTAMP_LTZ(ts, 0) ," +
            "                              WATERMARK FOR et AS et - INTERVAL '1' SECONDS   ) " +
            "                       WITH (  " +
            "                         'connector' = 'filesystem',  " +
            "                         'path' = 'data/UserBehavior.csv',   " +
            "                         'format' = 'csv'    " +
            "                            )      ";

        tableEnv.executeSql(createTableSQL);

        //每5min计算过去1h商品的点击量   滑动窗口(group winodw | TVF window)
        String slideSql = "SELECT window_start, window_end, itemId , count(*) click " +
            "  FROM TABLE(" +
            "    HOP(TABLE t1, DESCRIPTOR(et), INTERVAL '5' MINUTES , INTERVAL '1' HOURS)) " +
            "  where behavior = 'pv' " +
            "  GROUP BY window_start, window_end, itemId";

        Table t2 = tableEnv.sqlQuery(slideSql);
        tableEnv.createTemporaryView("t2",t2);

        /*
                按照窗口进行分组，求相同窗口下，点击量最多的前3

                topN需要使用排名函数。
                    hive:  rank,dense_rank,row_number
                    flink: 只支持 row_number(开窗函数，需要和over一起使用)

                    sql中over order by 后面只能跟时间,单独使用报错
                            如果这个sql后面还有过滤sql,使用就不报错
         */
        String  topNSql = " select" +
            "                    window_start, window_end,    itemId , click ," +
            "                    row_number() over( partition by  window_end order by click desc  )  rn " +
            "               from t2  ";

        Table t3 = tableEnv.sqlQuery(topNSql);
        tableEnv.createTemporaryView("t3",t3);

        /*
                过滤前3名,将结果进行持久化

                选择Mysql数据库写入
                    mysql建表语句:
                     CREATE TABLE `hot_item` (
                    `w_start` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      `w_end` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      `item_id` BIGINT(20) NOT NULL,
                      `item_count` BIGINT(20) NOT NULL,
                      `rk` BIGINT(20) NOT NULL,
                      PRIMARY KEY (`w_end`,`rk`)
                    ) ENGINE=INNODB DEFAULT CHARSET=utf8;
         */

        //在flink中创建一个表，这个表使用 Connector连上Mysql的指定的表
        String mysqlTable = "CREATE TABLE `hot_item` (" +
            "`w_start` TIMESTAMP ," +
            "  `w_end` TIMESTAMP ," +
            "  `item_id` BIGINT ," +
            "  `item_count` BIGINT," +
            "  `rk` BIGINT," +
            "  PRIMARY KEY (`w_end`,`rk`) NOT ENFORCED " +
            ") WITH (" +
            "   'connector' = 'jdbc'," +
            "   'url' = 'jdbc:mysql://hadoop102:3306/gmall'," +
            "   'table-name' = 'hot_item' ," +
            "   'username' = 'root' , " +
            "   'password' = '123456' " +
            ")";

        tableEnv.executeSql(mysqlTable);

        //执行写操作
        tableEnv.executeSql("insert into hot_item  select * from  t3 where rn <=3 ");



    }
}
