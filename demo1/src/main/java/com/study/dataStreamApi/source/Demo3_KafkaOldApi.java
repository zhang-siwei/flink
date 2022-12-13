package com.study.dataStreamApi.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author zhang.siwei
 * @time 2022-12-13 11:34
 * @action  flink 1.4 之前连接kafka
 * *      读取外部的数据源： 各种数据库，kafka，API不是内置的，需要额外引用。
 *  *          引入一个Connector
 *  *
 *  *      ------------------------
 *  *          kafka中数据的格式:
 *  *              K-V:
 *  *                  K:  一般只用于分区。
 *  *                          K相同的，一定是位于同一个partition。
 *  *                          一般情况下，K是没有任何值的，具体有没有取决于生产者怎么去封装ProducerRecord。
 *  *                          Kafka-console-producer.sh : 生产的数据是没有key的。
 *  *
 *  *                          如果key不存储数据，在置顶反序列化器时，可以只置顶Value的反序列化器。
 *  *                          key的反序列化器是可选的！
 *  *
 *  *                  V:  真正存储数据的
 *  *
 *  *              producer ---> 数据 ----->ProducerRecord(topic,K,V) -----> 序列化 -----> broker ----> 反序列化 ----> ConsumerRecord(K,V)----> Consumer
 *  *
 *  *         ----------
 *  *          1.14标记为过时
 *  *                  1.14之前，创建source调用  env.addSource(SourceFunction s)
 */
public class Demo3_KafkaOldApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //封装消费者的参数
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink");
        //自动提交offsets
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500");

        //创建一个Flink程序中的kafka 消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("topicA", new SimpleStringSchema(), properties);

        //从哪个位置消费
        //consumer.setStartFromEarliest(); //从最早的位置消费
        consumer.setStartFromLatest();  //从最后的位置消费
        //consumer.setStartFromGroupOffsets(); //从组已经消费过的位置向后消费 ,如果第一次消费，默认是从latest位置消费。默认的
        //consumer.setStartFromSpecificOffsets(); //自定义位置
        //添加一个数据源
        DataStreamSource<String> streamSource = env.addSource(consumer);
        streamSource.print();
        env.execute();

    }
}
