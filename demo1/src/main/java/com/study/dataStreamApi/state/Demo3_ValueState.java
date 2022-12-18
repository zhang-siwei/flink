package com.study.dataStreamApi.state;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.crypto.spec.OAEPParameterSpec;

/**
 * @author zhang.siwei
 * @time 2022-12-18 19:49
 * @action  键控状态之 ValueState
 * 检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警
 * 使用当前传感器的水位和上一个传感器的水位比对。
 * 保存上一个传感器的水位(单个值)，单值类型的State
 * -------------------
 * State.get(): 查询状态的值，读
 * State.add(): 添加单个值
 * State.addAll(): 添加多个值
 * State.clear(): 清空
 * State.update():  先清空，再写入。类似覆盖写
 * MapState.put()
 * MapState.get()
 */
public class Demo3_ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ValueState<Integer> lastvc;

                    //在Task初始化时，获取状态。 open()是一个生命周期方法，可以在这个方法中完成状态的获取
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastvc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastvc", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                        if (waterSensor.getVc() > 10 && lastvc.value() != null && lastvc.value() > 10) {
                            collector.collect(context.getCurrentKey() + "连续两个传感水位上升");
                        }
                        //更新状态
                        lastvc.update(waterSensor.getVc());
                    }
                })
                .print();
        env.execute();
    }
}
