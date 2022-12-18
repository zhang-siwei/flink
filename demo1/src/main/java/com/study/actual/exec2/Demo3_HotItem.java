package com.study.actual.exec2;

import com.study.actual.function.UserBehaviorMapFun;
import com.study.actual.pojo.UserBehavior;
import com.study.function.MyUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author zhang.siwei
 * @time 2022-12-18 23:38
 * @action 每隔5分钟输出最近1小时 内 点击量最多的前N个商品
 */
public class Demo3_HotItem {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((u, l) -> u.getTimestamp() * 1000);
        env.readTextFile("data/UserBehavior.csv")
                .map(new UserBehaviorMapFun())
                .filter(u -> "pv".equals(u.getBehavior()))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AggregateFunction<UserBehavior, Integer, HotItem>() {
                               @Override
                               public Integer createAccumulator() {
                                   return 0;
                               }

                               @Override
                               public Integer add(UserBehavior userBehavior, Integer integer) {
                                   return integer += 1;
                               }

                               @Override
                               public HotItem getResult(Integer integer) {
                                   return new HotItem(null, null, null, integer);
                               }

                               @Override
                               public Integer merge(Integer integer, Integer acc1) {
                                   return null;
                               }
                           }, new ProcessWindowFunction<HotItem, HotItem, Long, TimeWindow>() {
                               //开窗中的process,仅执行一次
                               @Override
                               public void process(Long key, Context context, Iterable<HotItem> iterable, Collector<HotItem> collector) throws Exception {
                                   //取出聚合的结果
                                   HotItem hotItem = iterable.iterator().next();
                                   hotItem.setItemId(key);
                                   TimeWindow window = context.window();
                                   hotItem.setStart(window.getStart());
                                   hotItem.setEnd(window.getEnd());
                                   collector.collect(hotItem);
                               }
                           }
                )
                .keyBy(HotItem::getEnd)
                //不开窗的,每一条数据执行一次
                .process(new KeyedProcessFunction<Long, HotItem, String>() {

                    private ListState<HotItem> items;
                    //判断是否时窗口中的第一条的标记
                    private ValueState<Boolean> isFirst;

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        List<HotItem> top3 = StreamSupport.stream(items.get().spliterator(), true)
                                .sorted((h1, h2) -> -h1.getClick().compareTo(h2.getClick()))
                                .limit(3)
                                .collect(Collectors.toList());
                        HotItem hotItem = top3.get(0);
                        String windowStr = MyUtil.getTimeWindow(new TimeWindow(hotItem.getStart(), hotItem.getEnd()));
                        String top3Str = top3.stream().map(t -> t.getItemId() + "->" + t.getClick()).collect(Collectors.joining(","));
                        out.collect(windowStr + ":" + top3Str);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isFirst = getRuntimeContext().getState(new ValueStateDescriptor<>("isFirst", Boolean.class));
                        items = getRuntimeContext().getListState(new ListStateDescriptor<>("items", HotItem.class));
                    }

                    @Override
                    public void processElement(HotItem hotItem, Context context, Collector<String> collector) throws Exception {
                        TimerService timerService = context.timerService();
                        //如果第一条 idFirst里面保存的变量是没有赋值的
                        if (isFirst.value() == null) {
                            //注册定时器
                            timerService.registerEventTimeTimer(hotItem.getEnd() + 5000);
                            //更新标记
                            isFirst.update(false);
                        }
                        //把数据加入到集合中
                        items.add(hotItem);
                    }
                }).setParallelism(1)
                .print().setParallelism(1);
        env.execute();
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    private static class HotItem {

        //标识窗口的时间范围
        private Long start;
        private Long end;
        private Long itemId;
        private Integer click;

    }
}
