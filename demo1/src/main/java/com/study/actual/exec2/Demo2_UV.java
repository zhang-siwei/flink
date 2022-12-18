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

import java.util.HashSet;
import java.util.Set;

/**
 * @author zhang.siwei
 * @time 2022-12-18 23:30
 * @action 计算每小时的uv
 */
public class Demo2_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((u, l) -> u.getTimestamp()*1000);
        env.readTextFile("data/UserBehavior.csv")
                .map(new UserBehaviorMapFun())
                .filter(u -> "pv".equals(u.getBehavior()))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessAllWindowFunction<UserBehavior, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<UserBehavior> iterable, Collector<String> collector) throws Exception {
                        Set<Long> uids = new HashSet<>();
                        for (UserBehavior userBehavior : iterable) {
                            uids.add(userBehavior.getUserId());
                        }
                        collector.collect(MyUtil.getTimeWindow(context.window())+" UV "+uids.size());
                    }
                })
                .print().setParallelism(1);
        env.execute();
    }
}
