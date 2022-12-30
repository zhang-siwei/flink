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
 * @action  finlk sql 入门
 */
public class Demo1_SQL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());
        Table table = tableEnv.fromDataStream(ds);

        //执行查询
      /*  tableEnv.sqlQuery("select * from " + table + " where id = 's1' ")
                .execute()
                .print();*/

        //为动态表起个名字
        tableEnv.createTemporaryView("ws", table);

        tableEnv.sqlQuery("select * from ws where id='s1'")
                .execute()
                .print();
    }
}
