package com.study.cep;

import com.study.actual.pojo.LoginEvent;
import com.study.actual.pojo.UserBehavior;
import com.study.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @author zhang.siwei
 * @time 2022-12-29 16:48
 * @action
 *  * 如果同一用户（可以是不同IP）在2秒之内连续两次登录，都失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示
 */
public class Demo13_CEPLogin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<LoginEvent> watermarkStrategy = WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                .withTimestampAssigner((e,l)->e.getEventTime()*1000);
        KeyedStream<LoginEvent, Long> ds = env.readTextFile("data/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new LoginEvent(
                                Long.valueOf(split[0]),
                                split[1],
                                split[2],
                                Long.valueOf(split[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(LoginEvent::getUserId);
        //定义规则
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getEventType());
                    }
                })
                .times(2)
                .consecutive() //紧挨着的两条都是失败
                //下一条不在2s内到达,属于超时,超市不取,取未超时,正常匹配
                .within(Time.seconds(2));
        CEP.pattern(ds, pattern)
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    /*
                        key :规则名 fail
                        value : list<LoginEvent> pattern 匹配到的数据
                        如果匹配成功一定是2条
                     */
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        Long userId = map.get("fail").get(0).getUserId();
                        return userId+"恶意登录......";
                    }
                })
                .print()
                .setParallelism(1);
        env.execute();
    }
}
