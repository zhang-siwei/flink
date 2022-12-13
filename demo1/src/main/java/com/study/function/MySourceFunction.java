package com.study.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


/**
 * @author zhang.siwei
 * @time 2022-12-13 18:22
 * @action   自定义实现数据源
 *
 * * { userId:100,behavior:"uninstall","channel":"ZTE",timestamp:xxx  }
 *  * 其中behavior和channel来自如下枚举:
 *  *  behavior: "install","uninstall","download","update"
 *  *  channel:"ZTE","APPLE","HUAWEI","MI","VIVO","OPPO"
 *  *
 *  *
 *  *  SourceFunction: 是一个非并行的Source，parilisim只能是1
 *  *  ParallelSourceFunction: 是并行的Source
 */
//public class MySourceFunction implements SourceFunction<String> {
public class MySourceFunction implements ParallelSourceFunction<String> {
    boolean ifContinue = true;

    //持续产生数据
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        String [] behaviors = { "install","uninstall","download","update"};
        String [] channels = { "ZTE","APPLE","HUAWEI","MI","VIVO","OPPO"};

        while (true){
            //产生一条json JSONObject就是map
            JSONObject jsonObject = new JSONObject();
            //userId是1-100之间
            jsonObject.put("userId", RandomUtils.nextInt(1,1001));
            jsonObject.put("behavior", behaviors[RandomUtils.nextInt(0, behaviors.length)]);
            jsonObject.put("channels", channels[RandomUtils.nextInt(0, channels.length)]);
            jsonObject.put("timestamp", System.currentTimeMillis());
            //输出
            sourceContext.collect(jsonObject.toJSONString());
            Thread.sleep(500);
        }
    }
    //停止产生数据
    @Override
    public void cancel() {
        ifContinue=false;
    }
}
