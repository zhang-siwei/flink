package com.study.actual.exec2;

import com.study.actual.pojo.AdsClickLog;
import com.study.function.MyUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author zhang.siwei
 * @time 2022-12-19 11:07
 * @action 统计当天用户对广告的点击量，并统计拉黑的用户。
 * 限制用户每一天对每个广告的点击次数为100次。
 *   要求： 数据必须按照时间顺序有序。
 *           用户1 在12-17 点击了 a广告  100
 *           用户1 在12-18 点击了 a广告  1
 */
public class Demo4_AdsCLick {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("data/AdClickLog.csv")
                .map(new MapFunction<String, AdsClickLog>() {
                    @Override
                    public AdsClickLog map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new AdsClickLog(
                                Long.valueOf(split[0]),
                                Long.valueOf(split[1]),
                                split[2],
                                split[3],
                                Long.valueOf(split[4]));
                    }
                }).setParallelism(1)
                .keyBy(ads -> ads.getUserId() + "_" + ads.getAdsId())
                .process(new KeyedProcessFunction<String, AdsClickLog, String>() {
                    //声明变量，当前用户对某个广告的点击次数
                    private ValueState<Integer> count;
                    //声明变量，代表是否已经拉黑，已经拉黑后续点击不统计
                    private ValueState<Boolean> isBlack;
                    //声明变量，用来标识日期的变化，如果日期变化就重置拉黑的状态及点击次数
                    private ValueState<String> lastDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext context = getRuntimeContext();
                        isBlack = context.getState(new ValueStateDescriptor<>("isBlack", Boolean.class));
                        lastDate = context.getState(new ValueStateDescriptor<>("lastDate", String.class));
                        count = context.getState(new ValueStateDescriptor<>("count", Integer.class));
                    }



                    @Override
                    public void processElement(AdsClickLog value, KeyedProcessFunction<String, AdsClickLog, String>.Context ctx, Collector<String> out) throws Exception {
                        String currentDt = LocalDateTime.ofInstant(Instant.ofEpochSecond(value.getTimestamp()), ZoneId.of("GMT+8")).format(MyUtil.getDateTimeFormatter());
                        String lastDt = this.lastDate.value();
                        //判断当前是否已经跨天,如果跨天,一切统计要重置
                        if (lastDt == null || !lastDt.equals(currentDt)) {
                            isBlack.update(false);
                            count.update(0);
                            lastDate.update(currentDt);
                        }
                        //累加
                        count.update(count.value() + 1);
                        Integer click = count.value();
                        //获取结果判断
                        if (!isBlack.value() && click >= 100) {
                            //符合拉黑条件
                            out.collect(ctx.getCurrentKey() + ":" + click + ",已拉黑");
                            isBlack.update(true);
                        } else {
                            if (click < 100) {
                                //正常条件
                                out.collect(ctx.getCurrentKey() + ":" + click);
                            }
                        }
                    }
                })
                .print().setParallelism(1);
        env.execute();
    }
}
