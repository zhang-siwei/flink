package com.study.dataStreamApi.state;

import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhang.siwei
 * @time 2022-12-17 9:57
 * @action  状态,ListState,UnionListState
 * 每输入一个字符串，缓存到一个List中，打印list
 * ---------------
 * JobException: Recovery(恢复) is suppressed(抑制) by NoRestartBackoffTimeStrategy
 *                     目前Job尝试取恢复的，但是被某个策略抑制了。
 *          Task的failover:  某个Task的故障处理。
 * -----------------
 *     把flink的状态当做普通集合使用。
 *     ----------------------
 *       状态存储到哪里了?
 *           1.13之前的版本，状态管理的所有功能都是称为 状态后端。
 *
 *           1.13开始，把功能细化，细化为
 *
 *               StateBackEnd状态后端： 专注于状态的读写。
 *                       默认HashMapStateBackEnd。
 *                               所有的状态，默认存储在 TaskManager的堆内存中。
 *                               优点：  存储在堆内存中，效率高(读写内存)。
 *
 *                               缺点：  存储小状态。
 *
 *
 *                        可选: RocksDB
 *                               RocksDB是一个嵌入式数据库。
 *                                   嵌入式:  程序自带。启动程序，就有了数据库。 derby, superset内置了一个sqllite(安卓)。
 *                                           模式类似hbase。
 *                                               列族。 读写缓存，数据优先写入到写缓存，缓存满了刷写到磁盘中。
 *
 *                                   非嵌入式: 需要额外安装。 mysql,redis,hbase,ck
 *
 *                               优点:  存储大状态。
 *
 *                               缺点:  对比HashMap，效率低。需要读磁盘。
 *                                      所有的数据必须转为byte[]进行读写，每次读写需要额外的序列化和反序列化的开销。
 *
 *
 *                        配置文件中配置:
 *                               state.backend rocksdb | hashmap
 *
 *      -------------------------------------------------------------------------
 *
 *               checkpoint:      专注于状态的备份。
 *                       默认 checkpoint 把状态 备份到 JobManager的堆内存中。
 *
 *                       可以选择把状态备份到外部设备(文件系统)
 *                           本地：
 *                           远程（HDFS）:
 *
 *
 *                          配置文件配置:
 *                               state.checkpoints.dir:  hdfs路径
 *
 *
 *      -------------------
 *       savepoint:  手动备份。 统一格式。
 *                       当前备份的时候，使用的是rockdb状态后端。在执行savepoint备份时，备份的并不是rocksdb特有的格式，
 *                       而是一种统一格式。
 *                       下次在启动job时，可以通过配置切换状态后端，切换为hashMap,切换后无缝对接！
 *
 *                       主动备份！
 *                       切换状态后端！
 *                       1.13之后！
 *
 *
 *       checkpoint: 自动备份。 有格式。
 *                       当前使用的是rockdb作为状态后端，ck帮你备份的数据就是rockdb独有的文件格式。
 *                       下次在恢复时，类路径下必须引入rocksdb，才能重新恢复成功。
 */
public class Demo1_State {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //设置状态后端   (默认)
        //env.setStateBackend(new HashMapStateBackend());

        //设置为rocksdb
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // int restartAttempts  重启的次数限制, Time delayInterval  每次重启时间间隔
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(1)));

        //备份到本地文件系统
        //env.getCheckpointConfig().setCheckpointStorage("file:///d:/ck");

        //备份到hdfs
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck2");


        //设置cancel后依旧保留快照
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        // 设置ck一致性语义 精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置barior发送频率
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // 设置Checkpoint的超时时间，未完成的ck handle信息会被JobManager删除
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 同一时间只允许一个 checkpoint 进行，设置流中同时存在的barior的数量
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);




        /*设置重启策略,
        int restartAttempts  重启的次数限制, Time delayInterval  每次重启时间间隔
        该方法只能重启,不能恢复数据*/
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(1)));

        /*
        * 无线次重启，状态数据可以进行持久化。重启之后，可以从外部设备上读取状态信息，恢复
        * 开启了checkpoint，频率是每隔2s备份一次
        * checkpoint： 备份，存档
        * */
        env.enableCheckpointing(2000);

        env.socketTextStream("localhost", 8888)
                .map(new MyMapFun())
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        if (value.contains("x")) {
                            throw new RuntimeException("出异常了.....");
                        }
                        System.out.println(value);
                    }
                });
        env.execute();
    }

    public static class MyMapFun implements MapFunction<String, String>, CheckpointedFunction {
        //状态，自己实现(没有用flink提供的api)，称为RawState。一旦程序挂掉，数据无法恢复！
        //使用Flink提供的状态api可以保证，程序挂掉后，重启，数据可以恢复！
        //List<String> list = new ArrayList<>();

        //声明状态,flink的状态管理
        private ListState<String> listState;
        @Override
        public String map(String s) throws Exception {
            //向状态中添加一个元素
            listState.add(s);
            //获取状态中存储的元素
            Iterable<String> list = listState.get();
            return list.toString();
        }

        //把状态以快照形式保存，定期(参考checkpoint的频率)执行。自动帮你持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //System.out.println("MyMapFun.snapshotState");
        }

        //在每个Task启动时执行一次。 获取状态
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("MyMapFun.initializeState");
            //从备份的数据中，根据状态的描述，获取一个ListState
//            listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<String>("strList", String.class));;

            /*
                UnionListState 和 ListState的区别在于 状态的重分布。
                    ListState： 均匀分布(负载均衡)
                    UnionListState ： 每个Task 全量分布

                    场景很少，只有一个场景，kafkaSource。

                    kafkaSource是一个消费者。
                            假设当前source算子有2个subTask，消费一个主题的4个分区。

                            source1 :  分配  p1,p3
                                                p1-20,-p3-30
                            source2:   分配  p2,p4
                                                p2-10,p4-50
                       ------------------------
                        source挂掉，重启。
                            ource算子有2个subTask，消费一个主题的4个分区。会使用unionListState保存消费的offsets信息
                                source1 :  分配  p1,p4
                                                p1-20,-p3-30， p2-10,p4-50
                                 source2:   分配  p2,p3
                                                p1-20,-p3-30， p2-10,p4-50


             */
            listState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<String>("unionlist", String.class));

        }
    }

}
