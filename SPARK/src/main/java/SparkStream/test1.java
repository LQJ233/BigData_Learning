package SparkStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import java.util.LinkedList;
import java.util.Queue;

public class test1 {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建Spark配置
        SparkConf conf = new SparkConf()
                .setAppName("DatabasePrintDemo")
                .setMaster("local[*]");

        // 2. 创建Streaming上下文（每1秒执行一次）
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // 3. 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 4. 数据库连接参数
        String jdbcUrl = "jdbc:mysql://localhost:3306/test01";
        String jdbcDriver = "com.mysql.cj.jdbc.Driver";
        String user = "root";
        // 密码通过环境变量提供，避免硬编码泄露；本地学习可在环境变量 MYSQL_PASSWORD 中配置（例如 export MYSQL_PASSWORD=你的密码）
        String password = System.getenv("MYSQL_PASSWORD");
        if (password == null) {
            throw new IllegalStateException("请先设置环境变量 MYSQL_PASSWORD 提供 MySQL 密码");
        }

        // 5. 创建虚拟队列（触发批次执行）
        LinkedList<JavaRDD<String>> dummyQueue = new LinkedList<>();
        dummyQueue.add(jssc.sparkContext().emptyRDD()); // 添加空RDD初始化


        // 6. 定义流处理逻辑
        jssc.queueStream(dummyQueue) // 传入队列参数
                .foreachRDD(rdd -> {
                    // 执行数据库查询
                    Dataset<Row> empDF = spark.read()
                            .format("jdbc")
                            .option("url", jdbcUrl)
                            .option("driver", jdbcDriver)
                            .option("dbtable", "emp")
                            .option("user", user)
                            .option("password", password)
                            .load();

                    // 打印数据
                    System.out.println("======= EMP表数据 =======");
                    empDF.show(false);
                });



        // 7. 启动流处理
        jssc.start();
        jssc.awaitTermination();
        for (JavaRDD<String> rdd : dummyQueue) {
            System.out.println(rdd.collect());
        }

    }
}