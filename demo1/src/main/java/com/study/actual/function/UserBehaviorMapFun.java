package com.study.actual.function;

import com.study.actual.pojo.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import scala.Int;

/**
 * @author zhang.siwei
 * @time 2022-12-13 21:03
 * @action
 */
public class UserBehaviorMapFun implements MapFunction<String, UserBehavior> {
    @Override
    public UserBehavior map(String s) throws Exception {
        String[] split = s.split(",");
        UserBehavior userBehavior = new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]),
                Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
        return userBehavior;
    }
}
