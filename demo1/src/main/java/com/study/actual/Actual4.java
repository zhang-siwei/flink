package com.study.actual;

import com.study.actual.pojo.AdsClickLog;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhang.siwei
 * @time 2022-12-15 15:55
 * @action  各省份页面广告点击量实时统计
 */
public class Actual4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.readTextFile("data/AdClickLog.csv")
                .map(new MapFunction<String, AdsClickLog>() {
                    @Override
                    public AdsClickLog map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new AdsClickLog(Long.valueOf(split[0]),
                                Long.valueOf(split[1]),
                                split[2], split[3], Long.valueOf(split[4])
                        );
                    }
                }).map(v -> new Tuple2<String,Integer>(v.getCity()+"_"+v.getAdsId(), 1))
                //使用lambda 返回tuple类型时
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(t -> t.f0)
                .sum(1).setParallelism(1)
                .print().setParallelism(1);
        env.execute();
    }
}
