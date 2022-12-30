package com.study.dataStreamApi.sqlfunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.rand;

/**
 * @author zhang.siwei
 * @time 2022-12-30 21:52
 * @action  函数的使用
 */
public class Demo1_FunctionUse {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createSql=" CREATE TABLE t1 (id string,ts bigint,vc int) " +
                " WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'data/t1', " +
                " 'format' = 'csv' " +
                " ) ";
        //建表(连接外部文件系统)
        tableEnv.executeSql(createSql);

        Table t1 = tableEnv.from("t1");
        //使用函数  官方提供 https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/functions/systemfunctions/
        //DSL(tableapi)中如何使用  调用随机函数
//        t1.select($("id"),rand($("vc")).as("rum"))
//                .execute()
//                .print();

        //sql中使用
        tableEnv.sqlQuery("select id,rand(vc) rum from t1")
                .execute()
                .print();
    }
}
