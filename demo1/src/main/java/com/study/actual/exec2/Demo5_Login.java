package com.study.actual.exec2;

import com.study.actual.pojo.LoginEvent;
import com.study.function.MyUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author zhang.siwei
 * @time 2022-12-19 16:01
 * @action 如果同一用户（可以是不同IP）在2秒之内连续两次登录，都失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示
 * 同一用户（可以是不同IP）在2秒之内连续两次登录，都失败
 *      和数量相关:
 *              2秒之内  :  2秒的窗口 不合适
 *              连续两次登录:  基于个数的窗口。 size = 2 ,slide = 1
 *                      保证数据是有序
 */
public class Demo5_Login {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("data/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new LoginEvent(
                                Long.valueOf(split[0]),
                                split[1],
                                split[2],
                                Long.valueOf(split[3]));
                    }
                })
                .keyBy(LoginEvent::getUserId)
                .countWindow(2, 1)
                .process(new ProcessWindowFunction<LoginEvent, String, Long, GlobalWindow>() {
                    @Override
                    public void process(Long key, ProcessWindowFunction<LoginEvent, String, Long, GlobalWindow>.Context context, Iterable<LoginEvent> elements, Collector<String> out) throws Exception {
                        List<LoginEvent> loginEvents = MyUtil.parseIterable(elements);
                        //在2s内连续登陆两次都失败
                        if (loginEvents.size() == 2) {
                            LoginEvent event1 = loginEvents.get(0);
                            LoginEvent event2 = loginEvents.get(1);
                            if ("fail".equals(event1.getEventType())
                                    && "fail".equals(event2.getEventType())
                                    && Math.abs(event1.getEventTime() - event2.getEventTime()) < 2
                            ) {
                                out.collect(key + "恶意登录..." + event1.getEventTime() + " , " + event2.getEventTime());
                            }
                        }
                    }
                })
                .print().setParallelism(1);
        env.execute();
    }
}
