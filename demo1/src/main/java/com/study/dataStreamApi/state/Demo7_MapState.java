package com.study.dataStreamApi.state;

import com.study.function.MyUtil;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-18 20:55
 * @action 键控状态之MapState
 * 去重： 每种传感器重复的水位值
 * Map：  key使用set存放，可以去重。
 */
public class Demo7_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private MapState<Integer, String> mapVc;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapVc = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, String>("mapVc", Integer.class, String.class));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        //vc作为状态放入,值不重要
                        mapVc.put(waterSensor.getVc(), "");
                        //获取去重后的key
                        collector.collect(context.getCurrentKey()+":"+ MyUtil.parseIterable(mapVc.keys()));
                    }
                })
                .print();
        ;
        env.execute();
    }
}
