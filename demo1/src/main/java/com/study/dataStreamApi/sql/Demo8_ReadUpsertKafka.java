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
 * @action 读取UpsertKafka ,可以显示更新变化
 */
public class Demo8_ReadUpsertKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

       /*
                建表和topicE映射

                   upsert-kafka 读取 无法指定读取的偏移量。只有从头读取，才知道整个数据的更新过程。
                                 如果当前有状态，启动后，从上次ck的状态位置读取。
                                 没有ck状态，启动后，从头读取！

                            显示 -U，+U，+I 记录，变化过程可以显示
         */
        String createSql = " CREATE TABLE t1 (id string,ts bigint,sumvc double,primary key(id,ts) not enforced) " +
                " WITH ( " +
                "   'connector' = 'upsert-kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "   'topic' = 'topicE'," +
                "   'key.format' = 'json'," +
                "   'value.format' = 'json' " +
                " ) ";

        // 普通的kafka只能将每一条数据作为 +I,读取。
        String kafkaSql = " CREATE TABLE t1( id string, ts bigint , vc double  ) " +
                "  WITH (  " +
                "    'connector' = 'kafka', " +
                "     'properties.bootstrap.servers' = 'hadoop102:9092'    , " +
                "      'properties.group.id' = 'test1' ,      " +
                "      'scan.startup.mode' = 'earliest-offset' ," +
                "    'topic' = 'topicE',   " +
                "    'format' = 'json'    " +
                "  )      ";

        tableEnv.executeSql(kafkaSql);

        tableEnv.sqlQuery("select * from t1")
                .execute()
                .print();
    }
}
