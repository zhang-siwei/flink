package com.study.dataStreamApi.sqlfunction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author zhang.siwei
 * @time 2022-12-30 22:40
 * @action  自定义聚合函数
 * UDAF: 聚合函数，输入多(行)，输出 1行1列
 */
public class Demo4_CustomUDAFFunction {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String  createTableSQL = " CREATE TABLE t1( id string, ts bigint , vc int  ) " +
                "                       WITH (  " +
                "                         'connector' = 'filesystem',  " +
                "                         'path' = 'data/t1',   " +
                "                         'format' = 'csv'    " +
                "                            )      ";
        tableEnv.executeSql(createTableSQL);
        Table t1 = tableEnv.from("t1");

        //①创建函数对象
        MyAvg myAvg = new MyAvg();

        //注册： 给函数起个名字，使用名字调用函数
        tableEnv.createTemporaryFunction("a",myAvg);

        t1
                .groupBy($("id"))
                .select($("id"),call("a",$("vc")).as("avgVc"))
                .execute()
                .print();

    }

    /* AggregateFunction<Double,MyAcc> 第一个参数是结果,第二个是累加器
    特点:
       下面几个方法是每个 AggregateFunction 必须要实现的：

            createAccumulator()
            accumulate()
            getValue()
     */
    public static class MyAvg extends AggregateFunction<Double,MyAcc>
    {

        //累加  第一个参数是累加器，第二个参数是输入的数据
        public void accumulate(MyAcc acc, Long vc) {
            acc.sum += vc;
            acc.count += 1;
        }


        //返回最终结果
        @Override
        public Double getValue(MyAcc accumulator) {
            return accumulator.sum / accumulator.count;
        }

        @Override
        public MyAcc createAccumulator() {
            return new MyAcc(0,0d);
        }
    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MyAcc{
        private Integer count;
        private Double sum;
    }
}
