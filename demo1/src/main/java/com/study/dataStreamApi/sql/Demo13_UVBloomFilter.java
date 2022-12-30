package com.study.dataStreamApi.sql;

import com.study.actual.pojo.UserBehavior;
import com.study.function.MyUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by Smexy on 2022/12/14
 *
 *    计算每小时的pv,使用BloomFilter
 *
 */
public class Demo13_UVBloomFilter
{
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
            .withTimestampAssigner( (e, r) -> e.getTimestamp() * 1000);


        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        
                env
                   .readTextFile("data/UserBehavior.csv")
                   .map(new MapFunction<String, UserBehavior>()
                   {
                       @Override
                       public UserBehavior map(String value) throws Exception {
                           String[] words = value.split(",");
                           UserBehavior userBehavior = new UserBehavior(
                               Long.valueOf(words[0]),
                               Long.valueOf(words[1]),
                               Integer.valueOf(words[2]),
                               words[3],
                               Long.valueOf(words[4])
                           );
                           return userBehavior;
                       }
                   })
                   .assignTimestampsAndWatermarks(watermarkStrategy)
                   .filter(u -> "pv".equals(u.getBehavior()))
                   .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                   //按照用户ID去重
                   .process(new ProcessAllWindowFunction<UserBehavior, String, TimeWindow>()
                   {

                       //开窗后用process，是非滚动聚合。 随着窗口的运算触发，只触发一次
                       @Override
                       public void process(Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {

                           int sum = 0;
                           /*
                                使用set集合去重，可以使用布隆过滤器(效率更高)

                                Funnel<? super T> funnel,: 判断的元素的类型
                                int expectedInsertions,： 期望的bitmap的长度
                                 double f(fail processs percent)pp :  误判的精度，不是成功的精度
                            */
                           BloomFilter<Long> longBloomFilter = BloomFilter.create(Funnels.longFunnel(), 100 * 10000 , 0.01);

                           for (UserBehavior element : elements) {

                               //返回true代表添加成功
                               if (longBloomFilter.put(element.getUserId())) {
                                   //底层是bitmap，添加成功，说明bitmap中不存在这个userId
                                   sum ++;
                               }

                           }

                           out.collect(MyUtil.getTimeWindow(context.window())+ " UV "+ sum);

                       }
                   })
                   .print().setParallelism(1);

        
                try {
                            env.execute();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

    }
}
