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
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
 */
public class Demo1_State {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
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
