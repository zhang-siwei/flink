package com.study.dataStreamApi.sqlfunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Created by Smexy on 2022/12/23
 *
 *      使用flink在读取外部数据时，如果遇到不同的数据源下存在同名的库和表。那么一条sql就会产生歧义，
 *      需要在数据源之前，再添加一层目录（catalog），进行区分！
 *
 *              mysql；   mysql.a库.1表
 *              oracle；   oracle.a库.1表
 *
 *        所有的元数据信息，都在 catalog中管理！
 *              可以把catalog中的数据，存储在程序的内存中！ 程序一关闭，元数据信息就丢失，在程序启动时使用！
 */
public class Demo6_ReadHive
{
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

        //获取当前默认的catalog  default_catalog
        System.out.println(tableEnv.getCurrentCatalog());
        //获取当前默认的库  default_database
        System.out.println(tableEnv.getCurrentDatabase());

        Table t1 = tableEnv.from("t1");

        /*tableEnv
                 .sqlQuery("select * from default_catalog.default_database.t1")
                 //.sqlQuery("select * from t1")
                .execute()
                .print();*/


        //创建一个可以读取hive的catalog
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "gmall", "data/conf");

        //注册hivecatalog
        tableEnv.registerCatalog("hiveCatalog",hiveCatalog);

        //切换到hive
        tableEnv.useCatalog("hiveCatalog");

        //读取hive中的表。 切换当前的catalog为 HiveCatalog才能读取hive中的表
        tableEnv
            .sqlQuery("select * from ods_log_inc limit 1")
            //.sqlQuery("select * from t1")
            .execute()
            .print();


    }
}
