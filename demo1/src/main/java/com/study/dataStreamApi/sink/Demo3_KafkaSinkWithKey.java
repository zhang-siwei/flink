package com.study.dataStreamApi.sink;

import com.alibaba.fastjson.JSON;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Hashtable;
import java.util.Properties;

/**
 * @author zhang.siwei
 * @time 2022-12-14 16:13
 * @action 不带key的 kafka sink  ,FlinkKafkaProducer
 *
 * key仅仅用于分区！
 *         key相同的，一定写入到kafka的同一个分区！
 */
public class Demo3_KafkaSinkWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> operator = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        /*
           FlinkKafkaProducer(
            String defaultTopic,   //写入的默认主题，基本用不上。
            KafkaSerializationSchema<IN> serializationSchema,  //KEY-VALUE如何序列化
                    IN: 流中输入的类型。 sink的上游输出的数据类型
            Properties producerConfig, //额外的参数
            FlinkKafkaProducer.Semantic semantic  //语义
            )
         */
        FlinkKafkaProducer<WaterSensor> kafkaProducer = new FlinkKafkaProducer<>("", new KafkaSerializationSchema<WaterSensor>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(WaterSensor waterSensor, @Nullable Long aLong) {
                //key: 取 id。同一种类型的传感器在kafka的一个分区
                byte[] key = waterSensor.getId().getBytes(StandardCharsets.UTF_8);
                //为了在外部软件中查看方便，将WaterSensor先转为String，再转为byte[]
                byte[] value = JSON.toJSONString(waterSensor).getBytes(StandardCharsets.UTF_8);
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topicC", key, value);
                return producerRecord;
            }
        }, properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        operator.addSink(kafkaProducer);
        env.execute();
    }
}
