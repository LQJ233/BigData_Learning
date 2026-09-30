package org.learning;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class Flink01_从mysql读取数据 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        // 定义 JDBC 输入格式
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver") // 数据库驱动
                .setDBUrl("jdbc:mysql://localhost:3306/mydatabase") // 数据库连接 URL
                .setUsername("root") // 数据库用户名
                .setPassword("527806") // 数据库密码
                .setQuery("SELECT id, name FROM mytable") // 查询语句
                .setRowTypeInfo(new JdbcRowTypeInfo( // 定义返回的字段类型
                        new TypeInformation[]{TypeInformation.of(Integer.class), TypeInformation.of(String.class)},
                        new String[]{"id", "name"}
                ))
                .setConnectionOptions(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withMaxRetryInterval(5000) // 最大重试间隔
                        .withConnectionCheckTimeoutMillis(5000) // 连接超时时间
                        .build())
                .finish();

        // 将 JDBC 数据源转换为 DataStream
        env.createInput(jdbcInputFormat)
                .map(row -> Tuple2.of((Integer) row.getField(0), (String) row.getField(1))) // 转换为 Tuple2
                .print(); // 打印到控制台

        // 执行程序
        env.execute("Flink JDBC Stream Example");




    }
}
