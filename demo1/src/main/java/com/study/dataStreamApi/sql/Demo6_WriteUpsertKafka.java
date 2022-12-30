package com.study.dataStreamApi.sql;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-30 18:51
 * @action  写入UpsertKafka
 */
public class Demo6_WriteUpsertKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        Table table = tableEnv.fromDataStream(ds);
        tableEnv.createTemporaryView("source", table);
         /*
            创建目标表
            insert into 目标表 select * from 输入表
            'upsert-kafka' tables require to define a PRIMARY KEY constraint.
       */
        String createSql = " CREATE TABLE t1 (id string primary key,sumvc double) " +
                " WITH ( " +
                "   'connector' = 'upsert-kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "   'topic' = 'topicD'," +
                "   'key.format' = 'json'," +
                "   'value.format' = 'json' " +
                " ) ";
        tableEnv.executeSql(createSql);

         /*
            执行insert的时候，写出的字段的名字是随便写的！
            只参考写出的字段的顺序，和目标表字段的顺序一一对应


             Table sink 't1'
             doesn't support consuming update changes
             which is produced by node GroupAggregate(groupBy=[id], select=[id, SUM(vc) AS a])

             流有三种:
                    只有insert : Append-Only 流
                    有update或delete:  使用 Changelog流 或 Retract流
                            作为 sink，upsert-kafka 连接器可以消费 changelog 流

                    普通的kafka连接器，只能写 Append-Only数据，如果聚合，意味聚合结果不要不断update，应该使用

                 insert(+I),和更新后的数据(+U)，以正常的数据写入。 Record(K=主键,v=数据)
                 delete数据，写入一条  Record(K=主键,v=null)，标识当前这个key的数据已经被删除。
                        kafka是消息系统，不支持删除数据！只能以追加的方式声明删除行为！

       */
         tableEnv.executeSql("insert into t1 select id,sum(vc) a from source group by id");
    }
}
