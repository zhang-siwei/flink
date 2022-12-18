package com.study.dataStreamApi.state;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-17 14:01
 * @action 广播流
 * 适用于动态更新配置的场景。
 * 处理数据流： 数据流
 * 规则(属性)配置的流: 配置流， 做成 广播流
 * 数据流在处理数据时，读取配置流中的信息。配置流中的信息可能需要动态变化，希望配置流中的信息一旦变化，可以广播到所有的数据流的Task中，
 * 让数据流用最新更新后的配置再进行处理。
 */
public class Demo2_BroadCastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //获取数据流
        SingleOutputStreamOperator<WaterSensor> dataDs = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());
        //获取配置流
        SingleOutputStreamOperator<myConf> confDs = env.socketTextStream("hadoop102", 8889)
                .map(new MapFunction<String, myConf>() {
                    @Override
                    public myConf map(String s) throws Exception {
                        String[] s1 = s.split(" ");
                        return new myConf(s1[0], s1[1]);
                    }
                });
        //制作配置流为广播流
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("conf", String.class, String.class);
        BroadcastStream<myConf> broadcastStream = confDs.broadcast(stateDescriptor);

        dataDs.connect(broadcastStream)
                .process(new BroadcastProcessFunction<WaterSensor, myConf, WaterSensor>() {
                    //处理数据流
                    @Override
                    public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, myConf, WaterSensor>.ReadOnlyContext ctx, Collector<WaterSensor> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                        String newValue = broadcastState.get(value.getId());
                        value.setId(newValue);
                        out.collect(value);
                    }

                    //处理广播流
                    @Override
                    public void processBroadcastElement(myConf value, BroadcastProcessFunction<WaterSensor, myConf, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        //获取广播流
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                        //把当前广播流中新流入的数据存入状态,自动的广播到所有处理数据流的task
                        broadcastState.put(value.name, value.getValue());
                    }
                })
                .print();
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class myConf {
        private String name;
        private String value;
    }
}
