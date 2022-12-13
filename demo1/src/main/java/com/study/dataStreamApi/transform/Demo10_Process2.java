package com.study.dataStreamApi.transform;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhang.siwei
 * @time 2022-12-13 19:49
 * @action 算子 process 练习
 * *  process:
 *  *          最底层算子。 强大，灵活，需要编程的多！
 *  *   -----------------------------
 *  *      希望统计每一种传感器的vc和
 */
public class Demo10_Process2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                //KeyedProcessFunction<K, I, O>
                //相同的key一定发送下游的同一个subTask
                //不同的key，有可能发往下游的同一个subTask
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //每一种key有自己累加的变量
                    Map<String,Integer> vcs = new HashMap<>();
                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        String currentKey = context.getCurrentKey();
                        //判断key是什么类型，累加到对应类型之前累加的结果
                        Integer sum = vcs.getOrDefault(currentKey, 0);
                        sum+= waterSensor.getVc();
                        vcs.put(currentKey, sum);
                        collector.collect(currentKey+":"+sum);
                    }
                }).setParallelism(2)
                .print();
        env.execute();
    }
}
