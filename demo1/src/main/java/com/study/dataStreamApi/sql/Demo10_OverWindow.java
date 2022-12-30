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
public class Demo10_OverWindow {
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

        //基于row的窗口
        String sql1 ="select id,ts,vc,sum(vc) over(partition by id order by et rows between unbounded preceding and current row) from ws ";
        String sql2 ="select id,ts,vc,sum(vc) over(partition by id order by et rows between 2 preceding and current row) from ws ";

        //基于range的窗口   eventtime  < watermark ，没有机会再去算，属于迟到的数据！
        String sql3 ="select id,ts,vc,sum(vc) over(partition by id order by et rows between unbounded preceding and current row) from ws ";
        String sql4 ="select id,ts,vc,sum(vc) over(partition by id order by et rows between interval '2' seconds preceding and current row) from ws ";

        //进行多个窗口函数的同时计算
        //TableException: Over Agg: Unsupported use of OVER windows. All aggregates must be computed on the same window.
        // 多个函数，计算的over()窗口必须一致！ 否则报错！
        String sql5 = " select id,ts,vc, " +
                "            sum(vc) over(partition by id order by et range between interval '2' seconds preceding and current row  )  sumVC ," +
                "            max(vc) over(partition by id order by et range between interval '2' seconds preceding and current row  )  maxVC " +
                "           from ws   ";
        //将窗口单独提出,进行简写
        String sql6 = " select id,ts,vc, " +
                "            sum(vc) over w  sumVC ," +
                "            max(vc) over w  maxVC " +
                "           from ws   " +
                //将窗口单独定义
                "           window w as (partition by id order by et range between interval '2' seconds preceding and current row  )  ";

        tableEnv.sqlQuery(sql6)
                .execute()
                .print();
    }
}
