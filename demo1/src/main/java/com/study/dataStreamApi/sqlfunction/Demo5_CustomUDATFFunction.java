package com.study.dataStreamApi.sqlfunction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author zhang.siwei
 * @time 2022-12-30 22:46
 * @action UDATF: 表生成聚合函数，  输入多(行)，输出 N行N列
 */
public class Demo5_CustomUDATFFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createTableSQL = " CREATE TABLE t1( id string, ts bigint , vc int  ) " +
                "                       WITH (  " +
                "                         'connector' = 'filesystem',  " +
                "                         'path' = 'data/t1',   " +
                "                         'format' = 'csv'    " +
                "                            )      ";

        tableEnv.executeSql(createTableSQL);
        Table t1 = tableEnv.from("t1");
        //①创建函数对象
        MyTop2 myAvg = new MyTop2();

        //注册： 给函数起个名字，使用名字调用函数
        tableEnv.createTemporaryFunction("a", myAvg);

        t1
                .groupBy($("id"))
                //调用,使用flatAggregate, 表生成聚合函数  只提供DSL的写法，没有sql
                .flatAggregate(call("a", $("vc")))
                //查询字段的名是函数中返回值的字段名,即myRow的字段
                .select($("id"), $("rank"), $("vc"))
                .execute()
                .print();

    }

    /*
    每一条数据到来后，求当前最大的两个水位。
            输入 s1,100,2  输出:  1，2
            输入 s1,100,3 输出:   1,3
                                2,2


    特点:
       下面几个方法是每个 TableAggregateFunction 必须要实现的：

           createAccumulator()
           accumulate()
     */
    public static class MyTop2 extends TableAggregateFunction<MyRow, MyAcc> {

        //累加  无需返回。第一个参数累加器，第二个输入的数据
        public void accumulate(MyAcc acc, Long vc) {

            if (vc > acc.firstVc) {
                //把当前水位提升为第一名，把之前的第一名降级为第二名
                acc.setSecondVc(acc.firstVc);
                acc.setFirstVc(vc);
            } else if (vc > acc.secondVc) {
                //把当前的水位设置为第二名
                acc.setSecondVc(vc);
            }
        }


        //输出最终的数据  第一个参数是累加器，第二个是Collector<输出的一行的类型>
        public void emitValue(MyAcc acc, Collector<MyRow> out) {
            out.collect(new MyRow(1L, acc.firstVc));
            //如果有第二名，再输出，否则不输出
            if (acc.secondVc > 0) {
                out.collect(new MyRow(2L, acc.secondVc));
            }

        }

        @Override
        public MyAcc createAccumulator() {
            return new MyAcc();
        }
    }


    //上一次运输后第一名和第二名的水位值,累加器
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MyAcc {
        private Long firstVc = 0L;
        private Long secondVc = 0L;
    }

    //输出的一行
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MyRow {
        private Long rank;
        private Long vc;
    }
}
