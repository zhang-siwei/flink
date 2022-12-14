package com.study.dataStreamApi.sink;

import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zhang.siwei
 * @time 2022-12-14 19:31
 * @action 自定义输出器
 * 希望把每种传感器统计的vc和写入mysql
 */
public class Demo4_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> operator = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .sum("vc");

        operator.addSink(new MySink());
        env.execute();

    }

    public static class MySink extends RichSinkFunction<WaterSensor> {

        private Connection connection;
        private PreparedStatement ps;
        String sql = "replace into ws values(?,?,?)";

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            ps.setString(1,value.getId() );
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
            //执行写出
            ps.execute();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/upm?useSSL=false&useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true"
                    , "root", "123456");
            ps = connection.prepareStatement(sql);
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
