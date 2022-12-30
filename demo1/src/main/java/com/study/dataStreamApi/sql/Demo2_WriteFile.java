package com.study.dataStreamApi.sql;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-30 16:53
 * @action  finlk sql 写入文件
 */
public class Demo2_WriteFile {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());
        Table table = tableEnv.fromDataStream(ds);

        //为动态表起个名字
        tableEnv.createTemporaryView("source", table);

         /*
            创建目标表
            insert into 目标表 select * from 输入表
       */
         String createSql=" CREATE TABLE t1 (id string,ts bigint,vc int) " +
                 " WITH ( " +
                     " 'connector' = 'filesystem', " +
                     " 'path' = 'data/t1', " +
                     " 'format' = 'csv' " +
                 " ) ";
        //建表(连接外部文件系统)
        tableEnv.executeSql(createSql);
        //把目标表写出到t1
        tableEnv.executeSql("insert into t1 select * from source");
    }
}
