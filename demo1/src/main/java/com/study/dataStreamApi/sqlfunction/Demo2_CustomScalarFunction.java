package com.study.dataStreamApi.sqlfunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zhang.siwei
 * @time 2022-12-30 21:52
 * @action 自定义标量函数
 *      ScalarFunction: UDF ,一(行)进一(行列)出
 */
public class Demo2_CustomScalarFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createSql=" CREATE TABLE t1 (id string,ts bigint,vc int) " +
                " WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'data/t1', " +
                " 'format' = 'csv' " +
                " ) ";
        //建表(连接外部文件系统)
        tableEnv.executeSql(createSql);
        Table t1 = tableEnv.from("t1");
        //①创建函数对象
        MyUpper myUpper = new MyUpper();
        //把id转为大写
        //②不注册直接用
       /* t1.select($("id"), Expressions.call(myUpper,$("id")))
          .execute()
          .print();*/

        //注册： 给函数起个名字，使用名字调用函数
        tableEnv.createTemporaryFunction("a",myUpper);

        /*t1.select($("id"), Expressions.call("a",$("id")))
          .execute()
          .print();*/

        //SQL  必须注册名字
        tableEnv.sqlQuery("select id ,a(id)  from t1")
                .execute()
                .print();
    }

    public static class MyUpper extends ScalarFunction {

        //求值方法必须是 public 的，而且名字必须是 eval
        // 必须有返回值
        // @DataTypeHint : 对参数进行类型提示，加快编译器编译的速度
        public String eval(@DataTypeHint("STRING") String s) {
            if (s != null){
                return s.toUpperCase();
            }else{
                return "NULL";
            }
        }
    }
}
