package com.study.dataStreamApi.sink;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zhang.siwei
 * @time 2022-12-14 19:51
 * @action *      把每种传感器的水位和写入ck
 * *          t1:   s1,200,100
 * *          t2:   s1,201,300
 * *
 * create table ws
 * (   id String,
 * ts UInt64,
 * vc UInt32
 * )engine=ReplacingMergeTree(ts)
 * order by (id);
 * <p>
 * <p>
 * JdbcSink.sink(
 * sqlDmlStatement,                       // mandatory
 * jdbcStatementBuilder,                  // mandatory
 * jdbcExecutionOptions,                  // optional。 自己构造
 * jdbcConnectionOptions                  // mandatory
 * );
 */
public class Demo5_JDBCSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> operator = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .sum("vc");

        SinkFunction<WaterSensor> sink = JdbcSink.sink("insert into ws values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement ps, WaterSensor waterSensor) throws SQLException {
                        ps.setString(1, waterSensor.getId());
                        ps.setLong(2, waterSensor.getTs());
                        ps.setInt(3, waterSensor.getVc());
                    }
                }, JdbcExecutionOptions.builder()
                        .withBatchSize(200).withBatchIntervalMs(500)
                        .withMaxRetries(3).build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://hadoop102:8123/default")
                        .build()
        );
        operator.addSink(sink);
        env.execute();
    }
}
