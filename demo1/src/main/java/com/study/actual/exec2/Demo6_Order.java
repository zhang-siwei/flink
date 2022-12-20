package com.study.actual.exec2;

import com.study.actual.pojo.OrderEvent;
import com.study.function.MyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author zhang.siwei
 * @time 2022-12-19 16:20
 * @action 需求: 找到超时(30min)和对账失败(只有支付信息没有订单信息)的订单。
 */
public class Demo6_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner((o, l) -> o.getEventTime() * 1000);
        env.setParallelism(1);
        env.readTextFile("data/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new OrderEvent(
                                Long.valueOf(split[0]),
                                split[1],
                                split[2],
                                Long.valueOf(split[3]));
                    }
                })
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(OrderEvent::getOrderId)
                .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
                .process(new ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>() {

                    private ValueState<String> orderId;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        orderId = getRuntimeContext().getState(new ValueStateDescriptor<>("orderId", String.class));
                    }
                    // elements 可能有2个，可能只有1个
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>.Context context, Iterable<OrderEvent> elements, Collector<String> out) throws Exception {
                        List<OrderEvent> orderEvents = MyUtil.parseIterable(elements);
                        if (orderEvents.size()==1){
                            OrderEvent orderEvent = orderEvents.get(0);
                            //可能是下单
                            if("create".equals(orderEvent.getEventType())){
                                orderId.update(orderEvent.getOrderId().toString());
                            }else {
                                //是支付
                                String orderIdStr = orderId.value();
                                if (orderIdStr==null){
                                    out.collect(orderEvent.getOrderId()+" 缺少create信息");
                                }else {
                                    out.collect(orderIdStr+" 支付超时");
                                }
                            }

                        }
                    }
                })
                .print().setParallelism(1);
        env.execute();
    }
}
