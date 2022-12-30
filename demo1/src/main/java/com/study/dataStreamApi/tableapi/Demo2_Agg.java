package com.study.dataStreamApi.tableapi;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhang.siwei
 * @time 2022-12-30 10:24
 * @action   tableapi 之 group ,撤回流
 */
public class Demo2_Agg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        Table table = tableEnv.fromDataStream(ds);

        Table result = table.groupBy($("id"))
                .select($("id"), $("vc").sum().as("sumVc"));

        /*
            Exception in thread "main" org.apache.flink.table.api.TableException:
                sumVC is not found in PojoType<com.atguigu.flink.pojo.WaterSensor,
                 fields = [id: String, ts: Long, vc: Integer]>

                 把表转换为流时，表中的列必须和 流中的POJO的属性一一对应

                 ----------------
              如果不新建POJO，可以使用提供的通用的类型 Row(把表的一行封装为一个Row)

         */
        //DataStream<Tuple2<Boolean, Row>> ds2 = tableEnv.toRetractStream(result, Row.class);
        //DataStream<Tuple2<Boolean, MySum>> ds2 = tableEnv.toRetractStream(result, MySum.class);

        /*
            Exception in thread "main" org.apache.flink.table.api.TableException:
                Table sink 'default_catalog.default_database.Unregistered_DataStream_Sink_1'
                doesn't support consuming update changes which is produced by node
                GroupAggregate(groupBy=[id], select=[id, SUM(vc) AS EXPR$0])

                聚合操作，会造成表中的数据会发生更新，目前toDataStream 默认只支持 Append-Only的数据。
                不支持消费更新的数据。

              +I[s1, 2]:  +I insert到表中一条记录
              -U[s1, 2]： -U update前
              +U[s1, 3]： +U update后

              toChangelogStream： 流使用log记录流中数据的变化记录

         */
        //DataStream<Row> ds2 = tableEnv.toChangelogStream(result);

        /*
                toRetractStream: 将变化的数据做成撤回流

                    Tuple2<Boolean, Row>： RetractStream的每行的value 和ChangelogStream 是一样的
                                           RetractStream多提供了一个Key，用来表明当前是否是最新的数据
         */
        DataStream<Tuple2<Boolean, MySum>> ds2 = tableEnv.toRetractStream(result, MySum.class);
        //对结果进行过滤,去除前面的key,只留数据
        /*ds2.filter(t -> t.f0)
                .map(t -> t.f1)
                .print();*/
        //ds2.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    //POJO不管是内部类还是外部类，权限必须是Public
    @Data
    @NoArgsConstructor
    public static class MySum{
        private String id;
        private Double sumVc;
    }
}
