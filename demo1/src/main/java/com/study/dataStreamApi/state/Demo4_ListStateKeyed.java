package com.study.dataStreamApi.state;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author zhang.siwei
 * @time 2022-12-18 20:05
 * @action  键控状态之ListStateListState
 * 针对每种传感器输出最高的3个水位值
 *  多值： ListState
 */
public class Demo4_ListStateKeyed {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ListState<Integer> top3;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        top3 = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                           //来一条数据和状态中已经有的数据进行比对取前3更新状态
                        top3.add(waterSensor.getVc());
                        //排序取最高的前3
                        List<Integer> top3list = StreamSupport.stream(top3.get().spliterator(), true)
                                .sorted(Comparator.reverseOrder())
                                .limit(3)
                                .collect(Collectors.toList());
                        //更新状态
                        top3.update(top3list);
                        collector.collect(context.getCurrentKey() + ":" + top3list.toString());
                    }
                })
                .print();
        env.execute();
    }
}
