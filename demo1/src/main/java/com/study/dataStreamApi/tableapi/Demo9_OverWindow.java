package com.study.dataStreamApi.tableapi;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author zhang.siwei
 * @time 2022-12-30 16:00
 * @action  tableapi 之 over
 */
public class Demo9_OverWindow {
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
        Table table = tableEnv.fromDataStream(ds,
                $("id"), $("ts"), $("vc"),
                $("pt").proctime(),  //处理时间
                $("et").rowtime()   //数据时间
        );

         /*
            语法:
                select
                    xxx, 窗口函数() over( partition by xxx order by xx  rows|range bettewn xxx and  xxx  )
                from 表

                rows: 以行数作为窗口的划分
                range： 以时间范围作为窗口的划分。

             ----------------------
                注意事项:  ValidationException: Ordering must be defined on a time attribute.
                                orderBy("时间属性")
         */
        //定义一个OverWindow   求同一种传感器，按照ts排序后的 sum(vc)

        //row  上无边界 到 当前行
        OverWindow w1 = Over.partitionBy($("id")).orderBy($("et")).preceding(UNBOUNDED_ROW).following(CURRENT_ROW).as("w");
        // row 前2行到当前行
        OverWindow w2 = Over.partitionBy($("id")).orderBy($("et")).preceding(rowInterval(2L)).following(CURRENT_ROW).as("w");
        //row 前2行到后2行  TableException: OVER RANGE FOLLOWING windows are not supported yet.  窗口的范围最多到当前行，无法向后扩展 错误示范
        OverWindow w3 = Over.partitionBy($("id")).orderBy($("et")).preceding(rowInterval(2L)).following(rowInterval(2L)).as("w");

        //range: 基于时间范围的窗口   窗口参考时间范围，从上无边界到当前的时间范围
        OverWindow w4 = Over.partitionBy($("id")).orderBy($("et")).preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE).as("w");
        //range: 基于时间范围的窗口   窗口参考时间范围，前2秒到当前的时间范围
        OverWindow w5 = Over.partitionBy($("id")).orderBy($("et")).preceding(lit(2).seconds()).following(CURRENT_RANGE).as("w");
        //range: 基于时间范围的窗口   窗口的下界只能到当前的时间范围  错误示范
        OverWindow w6 = Over.partitionBy($("id")).orderBy($("et")).preceding(lit(2).seconds()).following(lit(2).seconds()).as("w");


        table.window(w5)
                .select($("id"),$("ts"),$("vc").sum().over($("w")).as("sumVC"))
                .execute()
                .print();


    }
}
