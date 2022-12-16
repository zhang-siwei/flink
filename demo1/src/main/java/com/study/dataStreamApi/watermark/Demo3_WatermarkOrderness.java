package com.study.dataStreamApi.watermark;

import com.study.function.MyUtil;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

/**
 * @author zhang.siwei
 * @time 2022-12-16 19:54
 * @action
 *  * 水印策略:
 *  *          WatermarkStrategy<T> forMonotonousTimestamps() : 用于产生连续(不容忍乱序)的水印.
 *  *                          默认选 事件时间属性 - 1 作为水印
 *  *                                  等价于   forBoundedOutOfOrderness(0)
 *  *
 *  *          WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness)： 用于提供一个乱序容忍度，产生水印。
 *  *                      水印基于数据的时间，会推迟 maxOutOfOrderness
 *  *                      处理乱序(迟到)场景。
 *  *
 *  *    -----------------------
 *  *      数据有乱序(迟到的场景)：
 *  *              处理:
 *  *                      第一板斧:   推迟水印
 *  *                                      forBoundedOutOfOrderness(Duration maxOutOfOrderness)
 *  *
 *  *                                 推迟窗口计算的时机，一旦窗口计算了，窗口就关闭，关闭后，后续迟到的无法计算。
 *  *
 *  *                                 操作的对象时水印，设置水印的生成策略
 *  *
 *  *                      第二板斧:   当窗口计算后，不关闭，依旧等待迟到的数据。
 *  *                                  给他们一丝生机进入窗口。
 *  *
 *  *                                  操作的对象时窗口
 *  *
 *  *
 *  *                      第三板斧:   窗口关闭后，迟到的数据可以使用侧流接收。
 *  *                                  再进行进一步处理
 *  *
 *  *                                   操作的对象时窗口
 *  *
 *  *          ---------------------
 *  *              以上不能解决迟到的问题，说明乱序程度太大，从源头解决问题。否则建议批处理！
 */
public class Demo3_WatermarkOrderness {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(2000);

        OutputTag<WaterSensor> outputTag = new OutputTag<>("lateData", TypeInformation.of(WaterSensor.class));
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        return waterSensor.getTs();
                    }
                });
        SingleOutputStreamOperator<String> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy)
                //基于EventTime的开窗运算
                // [0,4999] =  [0,5000),   [5000,9999] = [5000,10000)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                //允许迟到，窗口计算后，迟到的数据依旧可以进入，进入后触发运算
                //.allowedLateness(Time.seconds(2)): 窗口到点触发运算后，再等2秒，这2秒不关闭
                //.allowedLateness(Time.seconds(2))
                // 对于关闭后迟到的数据，自动输出到侧流
                .sideOutputLateData(outputTag)
                .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        System.out.println(context.window());
                        List<WaterSensor> list = MyUtil.parseIterable(elements);
                        out.collect(list.toString());
                    }
                });
        //对主流,正常
        ds.print();
        //对测流,再处理
        ds.getSideOutput(outputTag)
                        .printToErr("迟到");
        env.execute();
    }
}
