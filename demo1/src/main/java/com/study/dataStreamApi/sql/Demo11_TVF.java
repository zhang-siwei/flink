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
 * @time 2022-12-30 20:41
 * @action  tvf
 */
public class Demo11_TVF {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
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
        tableEnv.createTemporaryView("ws", table);

        //滚动
        String tumbleSql="select window_start,window_end,id,sum(vc) sumVc" +
                " from table(" +
                " tumble(table ws,descriptor(et),interval '5' seconds))" +
                " group by window_start,window_end,id";
        //滑动
        String slideSql="select window_start,window_end,id,sum(vc) sumVc" +
                " from table(" +
                " hop(table ws,descriptor(et),interval '5' seconds,interval '10' seconds))" +
                " group by window_start,window_end,id";

        //1.13不支持session窗口。

        /*
                累积窗口。
                    每隔1h统计过去1天中累积PV
                     1:   0-1 PV
                     2:   0-1 + 1-2 PV
                     3:   0-1 + 1-2 + 2-3 PV

                     第一个 INTERVAL 是滑动步长     1h
                     第二个 INTERVAL 是窗口的最大范围  24h
         */
        String culSql="select window_start,window_end,id,sum(vc) sumVc" +
                " from table(" +
                " cumulate(table ws,descriptor(et),interval '2' seconds,interval '6' seconds))" +
                " group by window_start,window_end,id";

        tableEnv.executeSql(culSql)
                .print();
    }
}
