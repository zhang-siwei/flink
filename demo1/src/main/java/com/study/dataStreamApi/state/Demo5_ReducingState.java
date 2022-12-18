package com.study.dataStreamApi.state;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-18 20:23
 * @action 键控状态之 ReducingState
 * 计算每种传感器的水位和
 *  ReducingState； 聚合的类型和输出的类型一致！
 */
public class Demo5_ReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ReducingState<Integer> sumVc;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sumVc = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("sumVc", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer integer, Integer t1) throws Exception {
                                return integer + t1;
                            }
                        }, Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        //放入状态,,自动聚合
                        sumVc.add(waterSensor.getVc());
                        //取出聚合的结果
                        collector.collect(context.getCurrentKey() + ":" + sumVc.get());
                    }
                })
                .print();
        ;
        env.execute();
    }
}
