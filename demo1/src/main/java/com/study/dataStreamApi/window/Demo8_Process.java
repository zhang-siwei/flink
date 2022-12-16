package com.study.dataStreamApi.window;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author zhang.siwei
 * @time 2022-12-16 18:09
 * @action apply, process在窗口中，是非滚动聚合(齐了才计算)
 * 某些场景，需要使用非滚动聚合，例如求topN场景。
 * 求过去5个传感器中，vc最大的前三个
 */
public class Demo8_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .countWindowAll(5)
                .process(new ProcessAllWindowFunction<WaterSensor, String, GlobalWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        System.out.println("Demo8_Process.process");
                        //对elements中的数据排序求前3,对集合运算可以使用Streamapi
                        List<WaterSensor> top3 = StreamSupport.stream(elements.spliterator(), true)
                                .sorted((w1, w2) -> -w1.getVc().compareTo(w2.getVc()))
                                .limit(3)
                                .collect(Collectors.toList());
                        out.collect("前三:" + top3);
                    }
                })
                .print();
        env.execute();
    }
}
