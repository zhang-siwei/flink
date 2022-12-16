package com.study.dataStreamApi.window;

import com.study.function.MyUtil;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-16 11:43
 * @action
 * 按照性质：基于时间
 *     api: Window
 *             size： 由时间范围决定 [start,end)
 *             slide： 到点(end)就算。
 *           flink怎么知道时间是多少?
 *                 processingTime(演示):  处理时间。参考计算机的物理时钟！
 *                 eventTime:   从数据中提取属性作为时间。
 * 按照计算方式： keyBy
 *                 xxx
 * 按照功能:
 *         滑动|滚动|会话
 * --------------------
 *     规定窗口的范围是:  5s间隔
 * 每个key由自己的窗口。
 *         s1:  [0s,5s),[5s,10s)
 *         s2:  [0s,5s),[5s,10s)
 *         s3:  [0s,5s),[5s,10s)
 *     没到5s的整倍数，每种key的窗口都会触发计算。
 */
public class Demo4_TimeWindowKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                //基于处理时间的 滚动窗口。 从1970-1-1 0:0:0 每间隔10s就是一个窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        TimeWindow window = context.window();
                        MyUtil.printTimeWindow(window);
                        out.collect(MyUtil.parseIterable(elements).toString());
                    }
                }).print();
        env.execute();
    }
}
