package com.study.dataStreamApi.window;

import com.study.function.MyUtil;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-16 14:30
 * @action 对过去10s中进入窗口的传感器，求vc和。
 *      输出字符串
 *      Aggreate: 使用在输入和输出的类型不一致时。
 * ------------------------
 * 如果希望获取时间范围:
 *      reduce(ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function)
 *      aggregate(AggregateFunction<T, ACC, V> aggFunction, WindowFunction<V, R, K, W> windowFunction)
 * 运行原理:   reduce和aggregate都是滚动聚合。
 *          假设窗口输入: a,b,c ,聚合完之后得到结果 d
 *              reduce：   ((a ,b -----> reduce ), c -----> reduce )   ----> d  ---->
 *                  WindowFunction(只会在窗口计算时触发一次)
 * <p>
 *          WindowFunction： 输入: 是滚动聚合(ReduceFunction,AggregateFunction)输出的结果.
 *              可以在WindowFunction中对滚动聚合输出的结果再计算，加工，将最终的结果输出。
 * <p>
 *              例如： 顺便打印时间窗口的范围
 */
public class Demo7_Aggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(w -> "w")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        //AggregateFunction<IN, ACC, OUT>
                        new AggregateFunction<WaterSensor, Integer, String>() {
                            //构造一个累加器
                            @Override
                            public Integer createAccumulator() {
                                return 0;
                            }

                            //每输入一个元素，把计算的结果累加到累加器上
                            @Override
                            public Integer add(WaterSensor waterSensor, Integer acc) {
                                System.out.println("Demo7_Aggregate.add");
                                acc += waterSensor.getVc();
                                return acc;
                            }

                            //返回最终结果
                            @Override
                            public String getResult(Integer acc) {
                                return "过去10s所有传感器的vc和:" + acc;
                            }

                            //在流式计算中无需实现。只有在批处理的Api中才需要实现
                            @Override
                            public Integer merge(Integer integer, Integer acc1) {
                                return null;
                            }
                        },
                        //WindowFunction<IN, OUT, KEY, W extends Window>
                        new WindowFunction<String, String, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<String> input, Collector<String> out) throws Exception {
                                String result = input.iterator().next();
                                MyUtil.printTimeWindow(window);
                                out.collect("apply: " + result);
                            }
                        })
                .print();
        env.execute();
    }
}
