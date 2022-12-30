package com.study.dataStreamApi.tableapi;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.hive.shaded.parquet.Exceptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author zhang.siwei
 * @time 2022-12-29 17:09
 * @action  tableapi入门
 *
 * 核心的编程概念，和spark对比。
 * *                  编程环境                     编程模型
 * *      SparkCore   SparkContext                 RDD
 * *      SparkSql    SparkSession(SparkContext)   DataFrame|DateSet
 * *
 * flinkDataStreamAPI StreamExecutionEnvironment  DataStream
 * flinksql      StreamTableEnvironment(StreamExecutionEnvironment)   Table(动态表)
 */
public class Demo1_HelloWorld {
    public static void main(String[] args) {
        //获取编程环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        /*
            获取动态表
                可以直接定义
                从DataStream中转换得到
         */
        Table table = tableEnv.fromDataStream(ds);

        /*
            打印表结构

            (
              `id` STRING,
              `ts` BIGINT,
              `vc` INT
            )

         */
        table.printSchema();

        /*
                执行查询    希望执行什么sql操作，就调用什么方法。 举例： select----> Table.select()

                所有的方法都需要传入 多个 Expression
                        Expression： 代表操作逻辑。
                            获取：  Expressions.xxx() 返回 Expression

                 示例:
                        tab.select($("key"), $("value").avg().plus(" The average").as("average"))

                 sql：
                        select
                                key, avg(value) + The average   as  average
                        from  tab
         */
        // 对动态表运算后，生成新的动态表
        Table result = table.select(
                $("id"),
                $("ts"),
                $("vc")
        ).where($("id").isEqual("s1"));

        //直接操作表,不需要执行 env.execute();
        //result.execute().print();
        //接下来希望用 DataStreamAPI 或 更底层的 算子计算，把 Table转换为  DataStream
        //DataStream<WaterSensor> ds2 = tableEnv.toDataStream(result, WaterSensor.class);

        //流中的数据只会追加,不会更新
        DataStream<WaterSensor> ds2 = tableEnv.toAppendStream(result, WaterSensor.class);

        ds2.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
