package com.study.dataStreamApi.state;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-18 20:35
 * @action 键控状态之 AggregatingState
 * 计算每种传感器的平均水位
 */
public class Demo6_AggreagtingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private AggregatingState<Integer, Double> aggVc;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aggVc = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, MyAcc, Double>("aggVc",
                                new AggregateFunction<Integer, MyAcc, Double>() {
                                    @Override
                                    public MyAcc createAccumulator() {
                                        return new MyAcc();
                                    }

                                    @Override
                                    public MyAcc add(Integer integer, MyAcc myAcc) {
                                        myAcc.count += 1;
                                        myAcc.sum += integer;
                                        return myAcc;
                                    }

                                    @Override
                                    public Double getResult(MyAcc myAcc) {
                                        return myAcc.sum / myAcc.count;
                                    }

                                    @Override
                                    public MyAcc merge(MyAcc myAcc, MyAcc acc1) {
                                        return null;
                                    }
                                }, MyAcc.class));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        //放入状态,自动累加
                        aggVc.add(waterSensor.getVc());
                        //获取结果
                        collector.collect(context.getCurrentKey()+":"+aggVc.get());
                    }
                })
                .print();
        env.execute();
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class MyAcc {
        private Integer count = 0;
        private Double sum = 0d;
    }
}
