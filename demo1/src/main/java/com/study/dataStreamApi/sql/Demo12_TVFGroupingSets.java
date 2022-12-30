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
 * @action  tvf多维分析
 * 多维分析
 *         select
 *             xx
 *         from xxx
 *         group by a,b,c
 *         grouping sets( (a,b),(a),(b) )  hive有，flink TVF支持
 *         cube:  ck,hive有，flink TVF支持
 *         rollup: ck,hive有，flink TVF支持
 *         withTotal: ck中有
 */
public class Demo12_TVFGroupingSets {
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

        /*
                滚动

                如果要使用flink的 grouping sets 语法和hive和ck还不太一样。
                        group by 后面
                                hive :  group by  id(字段)
                                flink:  固定写法，只能写 窗口的起始和终止
         */
        String tumbleSql="select window_start,window_end,id,sum(vc) sumVc" +
                " from table(" +
                " tumble(table ws,descriptor(et),interval '5' seconds))" +
                " group by window_start,window_end," +
                //这三个的效果一样
                //"rollup((id))" +
                //"cube((id))" +
                "grouping sets((id),())";

        tableEnv.executeSql(tumbleSql)
                .print();
    }
}
