package com.study.dataStreamApi.sqlfunction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author zhang.siwei
 * @time 2022-12-30 22:14
 * @action 自定义表值函数
 *  TableFunction: UDTF ,一(行)进N(行N列)出
 *  特点:
 *         1.标量函数只能返回一个值不同的是，它可以返回任意多行。返回的每一行可以包含 1 到多列
 *
 *         2.如果输出行只包含 1 列，会省略结构化信息并生成标量值。 如果输出的行有多列，必须明确提供输出行中的结构信息(列名，列类型)
 *
 *         3.名为 eval 的方法.不同于标量函数，表值函数的求值方法本身不包含返回类型，而是通过 collect(T) 方法来发送要输出的行
 *
 *   使用:  在 Table API 中，表值函数是通过 .joinLateral(...) 或者 .leftOuterJoinLateral(...) 来使用的
 *                             .joinLateral(...)： inner join 取原表和炸裂后字段的交集
 *                             .leftOuterJoinLateral(...) ： left join 取原表的全部和 炸裂后字段的交集部分
 *
 *                 在 SQL 里面用 JOIN 或者 以 ON TRUE 为条件的 LEFT JOIN 来配合 LATERAL TABLE(<TableFunction>) 的使用。
 */
public class Demo3_CustomTableFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String  createTableSQL = " CREATE TABLE t1( id string, ts bigint , vc int  ) " +
                "                       WITH (  " +
                "                         'connector' = 'filesystem',  " +
                "                         'path' = 'data/t1',   " +
                "                         'format' = 'csv'    " +
                "                            )      ";
        tableEnv.executeSql(createTableSQL);
        Table t1 = tableEnv.from("t1");

        /*
           使用:  在 Table API 中，表值函数是通过 .joinLateral(...) 或者 .leftOuterJoinLateral(...) 来使用的
                            .joinLateral(...)： inner join 取原表和炸裂后字段的交集
                            .leftOuterJoinLateral(...) ： left join 取原表的全部和 炸裂后字段的交集部分

                在 SQL 里面用 JOIN 或者 以 ON TRUE 为条件的 LEFT JOIN 来配合 LATERAL TABLE(<TableFunction>) 的使用。


           -------------------------
            回忆hive，要使用udtf函数，例如explode，语法:

                select
                    id, col1,col2
                from t1
                lateral view explode(x) tmp as col1,col2

                本质：  lateral view 会被翻译为 join!。
                        UDTF如果希望和其他的列一起查询，必须使用Join，把炸裂的结果和一起查询的字段 join 起来！


         */
        //①创建函数对象
        //MySplit mySplit = new MySplit();
        MySplit2 mySplit = new MySplit2();

        //把id转为大写
        //②不注册直接用
        /*t1
            //.joinLateral(call(mySplit,$("id")))
            .leftOuterJoinLateral(call(mySplit,$("id")))
            .select($("id"), $("word"),$("length"))
            .execute()
            .print();*/

        //注册： 给函数起个名字，使用名字调用函数
        tableEnv.createTemporaryFunction("a",mySplit);

        /*t1
            .joinLateral(call("a",$("id")))
            //.leftOuterJoinLateral(call("a",$("id")))
            .select($("id"), $("word"),$("length"))
            .execute()
            .print();*/

        //SQL  必须注册名字
       /*tableEnv.sqlQuery("select id ,word,length" +
           //内连接
          // "             from t1 , lateral table( a(id) ) ")
           //"             from t1  inner join lateral table( a(id) )  on true")
               //左连接
           "             from t1  left join lateral table( a(id) )  on true")
                .execute()
                .print();*/

        //重命名字段
        tableEnv.sqlQuery("select id ,b,c" +
                //左连接
                "             from t1  left join lateral table( a(id) ) as tmp( b,c ) on true")
                .execute()
                .print();


    }

    /*
    特点:
        1.标量函数只能返回一个值不同的是，它可以返回任意多行。返回的每一行可以包含 1 到多列

        2.如果输出行只包含 1 列，会省略结构化信息并生成标量值。 如果输出的行有多列，必须明确提供输出行中的结构信息(列名，列类型)

        3.名为 eval 的方法.不同于标量函数，表值函数的求值方法本身不包含返回类型，而是通过 collect(T) 方法来发送要输出的行

        输入一个单词，按照_切割。输出N行2列，第一列是 单词 ，第二列是单词长度
        输入: s1   输出   s1
                         2

        <T> The type of the output row。 输出的一行的类型。
            输出的一行，有多列。
                    第一种： 把一行封装为 ROW
                    第二种： 列很多，封装为POJO
     */
    //声明炸裂的一行中的列的名字和类型
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class MySplit extends TableFunction<Row>
    {

        //求值方法必须是 public 的，而且名字必须是 eval
        // 必须无返回值
        // @DataTypeHint : 对参数进行类型提示，加快编译器编译的速度
        public void eval(@DataTypeHint("STRING") String s) {

            //如果是s3，不做处理。不输出
            if (!s.contains("s3")){
                String[] words = s.split("_");
                for (String word : words) {
                    collect(Row.of(word,word.length()));
                }
            }

        }

    }

    @FunctionHint(output = @DataTypeHint(bridgedTo = MyOut.class))
    public static class MySplit2 extends TableFunction<MyOut>
    {

        //求值方法必须是 public 的，而且名字必须是 eval
        // 必须无返回值
        // @DataTypeHint : 对参数进行类型提示，加快编译器编译的速度
        public void eval(@DataTypeHint("STRING") String s) {

            //如果是s3，不做处理。不输出
            if (!s.contains("s3")){
                String[] words = s.split("_");
                for (String word : words) {
                    collect(new MyOut(word,word.length()));
                }
            }

        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MyOut{
        private String word;
        private Integer length;
    }
}
