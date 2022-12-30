package com.study.dataStreamApi.sql;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhang.siwei
 * @time 2022-12-30 19:49
 * @action  时间窗口
 */
public class Demo10_GroupWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        //为了获取eventtime
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((w, l) -> w.getTs());

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);
        Table table = tableEnv.fromDataStream(ds,
                $("id"), $("ts"), $("vc"),
                $("pt").proctime(),  //处理时间
                $("et").rowtime()   //数据时间
        );

        //给表起名字
        tableEnv.createTemporaryView("ws", table);

         /*
        https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/group-agg/
                SQL不支持元素个数的窗口！

                滚动：  TUMBLE(time_attr, interval)
                滑动:  HOP(time_attr, slide interval, size interval)
                会话:  SESSION(time_attr, interval)
         */
        String tumbleSql = "select id," +
                "tumble_start(et,interval '5' second) wsstart," +
                "tumble_end(et,interval '5' second) wsend," +
                "sum(vc) sumVc " +
                "from ws " +
                "group by id,tumble(et,interval '5' second) ";

        //第一次触发时间不固定,但间隔固定
        String slideSql = "select id," +
                "hop_start(et,interval '3' second,interval '5' second) wsstart," +
                "hop_end(et,interval '3' second,interval '5' second) wsend," +
                "sum(vc) sumVc " +
                "from ws " +
                "group by id,hop(et,interval '3' second,interval '5' second) ";

        String slideSql2 = "select id," +
                "hop_start(pt,interval '3' second,interval '5' second) wsstart," +
                "hop_end(pt,interval '3' second,interval '5' second) wsend," +
                "sum(vc) sumVc " +
                "from ws " +
                "group by id,hop(pt,interval '3' second,interval '5' second) ";

        String sessionSql = "select id," +
                "session_start(et,interval '5' second) wsstart," +
                "session_end(et,interval '5' second) wsend," +
                "sum(vc) sumVc " +
                "from ws " +
                "group by id,session(et,interval '5' second) ";

        tableEnv.executeSql(sessionSql)
                 .print();
    }
}
