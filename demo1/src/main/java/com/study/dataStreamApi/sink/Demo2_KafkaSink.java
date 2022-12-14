package com.study.dataStreamApi.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author zhang.siwei
 * @time 2022-12-14 16:13
 * @action  不带key的 kafka sink
 *
 * FlinkKafkaProducer : 生产数据到kafka.
 *         ProducerRecord不需要key
 *         测试集群允许自动创建topic!
 *                 要写入的topic不存在，会自动创建。
 *                     自动创建的分区数和副本取决于broker的设置。
 */
public class Demo2_KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<String> operator = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .map(waterSensor -> JSON.toJSONString(waterSensor));

         /*
            FlinkKafkaProducer(
            String brokerList,  : 集群地址
            String topicId:     向哪个topic写入
             SerializationSchema<IN> serializationSchema:  序列化器
                    IN: 要写入的ProducerRecord中的value的类型。
                            一般都选择String(json)
             )
         */
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("hadoop102:9092", "topicB", new SimpleStringSchema());
        operator.addSink(kafkaProducer);
        env.execute();
    }
}
