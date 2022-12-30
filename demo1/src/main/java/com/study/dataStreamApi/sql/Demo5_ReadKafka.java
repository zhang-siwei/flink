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
 * @action finlk sql 写kafka
 */
public class Demo5_ReadKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



         /*
            创建目标表,和kafka进行映射
       */
        String createSql = " CREATE TABLE t1 (id string,ts bigint,vc int) " +
                " WITH ( " +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "   'topic' = 'topicD', " +
                "   'properties.group.id' = 'test1' ," +
                //读取位置
                "   'scan.startup.mode' = 'group-offsets' ," +
                "   'format' = 'json' " +
                " ) ";
        //建表(连接外部文件系统)
        tableEnv.executeSql(createSql);
        //写入数据
        tableEnv.sqlQuery("select * from t1")
                .execute()
                .print();
    }
}
