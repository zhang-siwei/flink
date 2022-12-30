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
 * @action  写入UpsertKafka NOT ENFORCED
 */
public class Demo7_WriteUpsertKafka2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        Table table = tableEnv.fromDataStream(ds);
        tableEnv.createTemporaryView("source", table);
         /*
            Exception in thread "main" org.apache.flink.table.api.ValidationException:

                    主键约束不支持 ENFORCED mode
                            ENFORCED mode： 强制模式。 当写入时，遵循主键的规则，一定出现主键冲突的，不能写入！
                          kafka，无法使用强制模式。

                Flink doesn't support ENFORCED mode for PRIMARY KEY constraint.

                ENFORCED/NOT ENFORCED  controls if the constraint checks are performed on the incoming/outgoing data.

                 Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode

         */
        String createSql = " CREATE TABLE t1 (id string,ts bigint,sumvc double,primary key(id,ts) not enforced) " +
                " WITH ( " +
                "   'connector' = 'upsert-kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "   'topic' = 'topicE'," +
                "   'key.format' = 'json'," +
                "   'value.format' = 'json' " +
                " ) ";
        tableEnv.executeSql(createSql);

         tableEnv.executeSql("insert into t1 select id,ts,sum(vc) a from source group by id,ts");
    }
}
