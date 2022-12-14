package com.study.dataStreamApi.sink;

import com.alibaba.fastjson.JSONObject;
import com.study.function.WaterSensorMapFunction;
import com.study.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author zhang.siwei
 * @time 2022-12-14 11:18
 * @action  redis sink
 *   5大数据类型:
 *    string:   set k1 v1
 *    set:      sadd k1,v1,v2,v3
 *    zset:     zadd k1 score member...
 *    list:     lpush  k1 v1
 *              rpush  k1  v2
 *    hash:     hset  k1  field value
 *  -----------------
 *    pubsub(发布订阅，类似kafka) ,hyperloglog(大基数统计)
 *    --------
 *        输出的套路：  DataStream.addSink(SinkFunction s);
 *
 */
public class Demo1_RedisSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> operator = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setDatabase(0)
                .setHost("hadoop103")
                .setPort(6379)
                .setTimeout(60000)
                .setMaxIdle(10)  // 池子中最大空闲的连接数
                .setMinIdle(5) // 池子中最小空闲的连接数
                .setMaxTotal(20)// 池子容量
                .build();

        //RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper)

        //输出到redis
//        operator.addSink(new RedisSink<>(flinkJedisPoolConfig, new StringRedisMapper()));
//        operator.addSink(new RedisSink<>(flinkJedisPoolConfig, new listRedisMapper()));
        operator.addSink(new RedisSink<>(flinkJedisPoolConfig, new HashRedisMapper()));
        env.execute();

    }
    public static class HashRedisMapper implements RedisMapper<WaterSensor>{
        /*
                写命令  zset:     zadd k1(additionalKey) score member...
                RedisCommandDescription(RedisCommand redisCommand, String additionalKey)： 用于构造 hash 和 zset类型
                RedisCommandDescription(RedisCommand redisCommand)： 用于构造非 hash 和 zset类型
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"ws_hash");
        }
        /*
             field
         */
        @Override
        public String getKeyFromData(WaterSensor waterSensor) {
            return waterSensor.getId();
        }
        //value
        @Override
        public String getValueFromData(WaterSensor waterSensor) {
            return waterSensor.toString();
        }
    }
    public static class ZsetRedisMapper implements RedisMapper<WaterSensor>{
        /*
                写命令  zset:     zadd k1(additionalKey) score member...
                RedisCommandDescription(RedisCommand redisCommand, String additionalKey)： 用于构造 hash 和 zset类型
                RedisCommandDescription(RedisCommand redisCommand)： 用于构造非 hash 和 zset类型
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.ZADD,"ws_zset");
        }
        /*
               member
          */
        @Override
        public String getKeyFromData(WaterSensor waterSensor) {
            return waterSensor.toString();
        }
        //score
        @Override
        public String getValueFromData(WaterSensor waterSensor) {
            return waterSensor.getVc().toString();
        }
    }
    public static class SetRedisMapper implements RedisMapper<WaterSensor>{
        /*
                写命令
                RedisCommandDescription(RedisCommand redisCommand, String additionalKey)： 用于构造 hash 和 zset类型
                RedisCommandDescription(RedisCommand redisCommand)： 用于构造非 hash 和 zset类型
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SADD);
        }
        /*
                        取WaterSensor的什么作为key

                        只保留不同传感器的最新的信息
                 */
        @Override
        public String getKeyFromData(WaterSensor waterSensor) {
            return waterSensor.getId();
        }
        //取WaterSensor的什么作为value
        @Override
        public String getValueFromData(WaterSensor waterSensor) {
            return waterSensor.toString();
        }
    }
    public static class listRedisMapper implements RedisMapper<WaterSensor>{
        /*
                写命令
                RedisCommandDescription(RedisCommand redisCommand, String additionalKey)： 用于构造 hash 和 zset类型
                RedisCommandDescription(RedisCommand redisCommand)： 用于构造非 hash 和 zset类型
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
        /*
                        取WaterSensor的什么作为key

                        只保留不同传感器的最新的信息
                 */
        @Override
        public String getKeyFromData(WaterSensor waterSensor) {
            return "list_"+waterSensor.getId();
        }
        //取WaterSensor的什么作为value
        @Override
        public String getValueFromData(WaterSensor waterSensor) {
            return waterSensor.toString();
        }
    }
    public static class StringRedisMapper implements RedisMapper<WaterSensor>{
        /*
                写命令
                RedisCommandDescription(RedisCommand redisCommand, String additionalKey)： 用于构造 hash 和 zset类型
                RedisCommandDescription(RedisCommand redisCommand)： 用于构造非 hash 和 zset类型
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }
        /*
                        取WaterSensor的什么作为key

                        只保留不同传感器的最新的信息
                 */
        @Override
        public String getKeyFromData(WaterSensor waterSensor) {
            return waterSensor.getId();
        }
        //取WaterSensor的什么作为value
        @Override
        public String getValueFromData(WaterSensor waterSensor) {
            return waterSensor.toString();
        }
    }
}
