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
 * @action  tableapi 之 groupby窗口
 */
public class Demo8_GroupWindow {
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
        https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/tableapi/#group-windows
            窗口运算
                GroupWindow:   Flink提供的窗口运算.
                                    功能:滚动，滑动，会话
                                    计算方式: KeyBy,不KeyBy
                                    类型:  基于时间，基于个数

       Tumble: 创建滚动窗口
            .over： length,size
            .on:   时间字段

                OverWindow:    Hive中提供的开窗函数   函数() over( partitioned by xx order by xx rows|range between xx and xxx  )
         */
        //--------------------------滚动窗口------------------------------------
        //Tumbling Event-time Window
        TumbleWithSizeOnTimeWithAlias w1 = Tumble.over(lit(5).seconds()).on($("et")).as("w");

        // Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
        TumbleWithSizeOnTimeWithAlias w2 = Tumble.over(lit(5).seconds()).on($("pt")).as("w");

        //  基于元素个数的窗口，也需要传入PT。
        TumbleWithSizeOnTimeWithAlias w3 = Tumble.over(rowInterval(5L)).on($("pt")).as("w");

        //--------------------------滑动窗口------------------------------------
        SlideWithSizeAndSlideOnTimeWithAlias w4 = Slide.over(lit(10).seconds()).every(lit(5).second()).on($("et")).as("w");
        SlideWithSizeAndSlideOnTimeWithAlias w5 = Slide.over(lit(10).seconds()).every(lit(5).second()).on($("pt")).as("w");

        // 第一次滑动计算时，必须满足窗口的size，才会触发！
        SlideWithSizeAndSlideOnTimeWithAlias w6 = Slide.over(rowInterval(3L)).every(rowInterval(2L)).on("pt").as("w");

        //--------------------------会话窗口------------------------------------
        // Session Event-time Window
        SessionWithGapOnTimeWithAlias w7 = Session.withGap(lit(3).seconds()).on($("et")).as("w");
        SessionWithGapOnTimeWithAlias w8 = Session.withGap(lit(3).seconds()).on($("pt")).as("w");

        // 时间窗口，滚动窗口，不keyBy
        table.window(w8)
                // keyBy的窗口，按照id
                .groupBy($("w"),$("id"))
                // 查询时，不支持使用 窗口作为查询字段。 如果是时间窗口，内含了[start,end)
                .select($("w").start(),$("w").end(),$("id"),$("vc").sum().as("sumVc"))
                //基于元素个数，没有 窗口的时间属性
                //.select($("id"),$("vc").sum().as("sumVC"))
                .execute()
                .print();


    }
}
