package com.study.dataStreamApi.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-30 19:11
 * @action  获取时间属性
 */
public class Demo9_DefineTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用sql定义表，指定表中某些字段为 pt和 et
        // https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/concepts/time_attributes/
        /*
                eventtime 如果是 2020-04-15 20:13:40格式，列的类型需要声明为 TIMESTAMP()
                            时间中有毫秒，写 TIMESTAMP(3)
                            没有毫秒，到秒结束，写TIMESTAMP(0)

                eventtime 如果是  1618989564564 格式， 列的类型需要声明为  TIMESTAMP_LTZ()
                        毫秒时间戳:  TIMESTAMP_LTZ(3)
                        秒时间戳:    TIMESTAMP_LTZ(0)


                et  AS TO_TIMESTAMP_LTZ(a, 3) ： 功能类似WatermarkStrategy.withTimestampAssigner( (e, r) -> e.getTs())


                 还需要分配水印！

                 WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND: 等价于  WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
         */
        String createSql = "create table t1(id string,a bigint,vc int," +
                "pt as proctime(),  et as TO_TIMESTAMP_LTZ(a,3)," +
                "watermark for et as et -interval '0.001' seconds)" +
                "with ( " +
                "'connector'='filesystem'," +
                "'path'='data/t1'," +
                "'format'='csv'" +
                ")";
        tableEnv.executeSql(createSql);
        tableEnv.sqlQuery("select * from t1")
                .execute()
                .print();
    }
}
