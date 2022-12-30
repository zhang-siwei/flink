package com.study.dataStreamApi.tableapi;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhang.siwei
 * @time 2022-12-30 15:53
 * @action   tableapi 之 获取时间戳
 */
public class Demo7_DefineTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        //为了获取eventtime
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        /*
            使用以下构造器
            fromDataStream(DataStream<T> dataStream, Expression... fields);

                生成Table，且为表添加 eventtime，processingTime

                    $("pt").proctime(): 调用 proctime()函数，生成一列，列名起名字为pt

                    $("et").rowtime(): 调用 rowtime()函数，生成一列，列名起名字为et
         */
        Table table = tableEnv.fromDataStream(ds,
                $("id"), $("ts"), $("vc"),
                $("pt").proctime(),  //处理时间
                $("et").rowtime()   //数据时间
        );

        tableEnv.sqlQuery("select * from "+table)
                .execute()
                .print();

    }
}
