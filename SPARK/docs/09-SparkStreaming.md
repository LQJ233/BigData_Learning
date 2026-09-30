# 09 · Spark Streaming：流式读取 MySQL（DStream API）

> 对应源码：[test1.java](../src/main/java/SparkStream/test1.java)
>
> **背景**：本例演示老一代的 Spark Streaming API（基于 DStream + `JavaStreamingContext`），通过一个**虚拟队列**触发定时批次，在每个批次中用 SparkSQL 的 JDBC 方式读取 MySQL 表并打印。生产上 Spark 3.x 更推荐 **Structured Streaming**（见文末）。

## 9.1 源码与逐行解读

> 对应源码：[test1.java](../src/main/java/SparkStream/test1.java)

```java
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
        // 1. 创建 Spark 配置
        SparkConf conf = new SparkConf()
                .setAppName("DatabasePrintDemo")
                .setMaster("local[*]");

        // 2. 创建 Streaming 上下文（每 10 秒执行一个批次）
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // 3. 创建 SparkSession（用于在批次内执行 JDBC 查询）
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

        // 5. 创建虚拟队列（用于触发批次执行）
        LinkedList<JavaRDD<String>> dummyQueue = new LinkedList<>();
        dummyQueue.add(jssc.sparkContext().emptyRDD()); // 添加空 RDD 初始化

        // 6. 定义流处理逻辑：每个批次执行一次 foreachRDD
        jssc.queueStream(dummyQueue) // 传入队列参数
                .foreachRDD(rdd -> {
                    // 在批次内用 SparkSQL 读取 MySQL 的 emp 表
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
                    empDF.show(false); // false：不截断列
                });

        // 7. 启动流处理
        jssc.start();
        jssc.awaitTermination(); // 阻塞等待任务终止
        for (JavaRDD<String> rdd : dummyQueue) {
            System.out.println(rdd.collect());
        }
    }
}
```

## 9.2 关键知识点

### ① JavaStreamingContext（老 API 入口）

- 通过 `new JavaStreamingContext(conf, Durations.seconds(10))` 创建，第二个参数是**批次间隔**（batch interval），即每隔 10 秒把流入的数据封装成一个 RDD 批次。
- 对应关系：`JavaSparkContext` → RDD 批处理入口；`JavaStreamingContext` → 流处理入口。

### ② queueStream + 虚拟队列（本示例的"数据源"技巧）

- `queueStream(Queue<JavaRDD<T>>)` 把内存中的 RDD 队列当作输入源，每次批次从队列中取出一个 RDD 处理。
- 本示例只放了一个**空 RDD**，目的不是消费数据，而是**制造一个每 10 秒触发一次的"心跳"**，让 `foreachRDD` 里的逻辑定时执行。
- 这是学习/演示「定时执行某段 Spark 逻辑」的常用技巧，生产环境不会这么用。

### ③ foreachRDD + SparkSQL JDBC（批次内查询数据库）

- `foreachRDD` 是 DStream 的"汇"算子：每个批次回调一次，参数是该批次的 RDD。
- 在回调里用 `spark.read().format("jdbc")` 读 MySQL：配置 `url`、`driver`、`dbtable`（表名）、`user`、`password` 即可，返回 `Dataset<Row>`。
- 效果：每 10 秒把 MySQL `emp` 表的最新数据打印一次（注意这里读的是**全表**，生产上应加增量条件）。

### ④ 执行模型（DStream 的微批思想）

```
数据流 → 按 10 秒切成微批（RDD）→ 每个批次执行一次 foreachRDD 中的逻辑
        └──────── 微批（micro-batch）模式 ────────┘
```

- Spark Streaming 不是"逐条"处理，而是**微批**：每 N 秒把这段时间的数据打包成一个 RDD 处理。因此实时性以秒级为粒度，不是毫秒级。

## 9.3 运行前提与注意

1. **本地 MySQL**：需要本机 3306 端口存在 `test01` 库和 `emp` 表，账号 root / 密码与源码一致（或自行修改）。
2. **数据库密码**：✅ 已改为环境变量 `MYSQL_PASSWORD` 读取，不再硬编码；本地运行时需先 `export MYSQL_PASSWORD=你的密码`。
3. **老 API 提示**：`JavaStreamingContext`/DStream 属于 Spark 1.x~2.x 的流处理 API，Spark 3.x 官方主推 **Structured Streaming**（`spark.readStream()`），语义更强、支持 Exactly-Once。本项目以学习 DStream 概念为主即可。

## 9.4 补充：Structured Streaming 的等价写法（了解即可）

```java
// 结构化流定时读 JDBC 的等价思路
// 结构化流目前对 JDBC source 支持有限，生产常用 Kafka/文件源 + 流式聚合：
// Dataset<Row> stream = spark
//         .readStream()
//         .format("kafka")
//         .option("kafka.bootstrap.servers", "hadoop102:9092")
//         .option("subscribe", "topic_1")
//         .load();
// stream.writeStream()
//         .outputMode("append")
//         .format("console")
//         .start()
//         .awaitTermination();
```
