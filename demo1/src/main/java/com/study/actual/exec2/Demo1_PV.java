package com.study.actual.exec2;

import com.study.actual.function.UserBehaviorMapFun;
import com.study.actual.pojo.UserBehavior;
import com.study.function.MyUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhang.siwei
 * @time 2022-12-18 23:02
 * @action 计算每小时的pv
 */
public class Demo1_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((u, l) -> u.getTimestamp() * 1000);
        env.readTextFile("data/UserBehavior.csv")
                .map(new UserBehaviorMapFun())
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .filter(u -> "pv".equals(u.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessAllWindowFunction<UserBehavior, String, TimeWindow>() {
                        //开窗后用process,是非滚动聚合,随着窗口的运算触发,只触发一次
                    @Override
                    public void process(Context context, Iterable<UserBehavior> iterable, Collector<String> collector) throws Exception {
                        int pvCount = MyUtil.parseIterable(iterable).size();
                        collector.collect(MyUtil.getTimeWindow(context.window())+" pv:"+pvCount);
                    }
                })
                .print().setParallelism(1);
        env.execute();
    }
}
