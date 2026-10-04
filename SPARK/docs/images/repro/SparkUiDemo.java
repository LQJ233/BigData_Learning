package demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

/**
 * 用来生成 Spark Web UI 截图的演示程序。
 *
 * 依次制造出 6 个页签需要的现场：
 *   1. RDD shuffle + cache      → Jobs / Stages / Storage
 *   2. 热点 Key 聚合            → Stage 详情里的 Task 时间分布（数据倾斜）
 *   3. DataFrame join           → SQL 页签的 DAG
 *   4. Spark SQL 聚合与窗口      → SQL 页签
 *   5. Structured Streaming     → Streaming 页签
 *   6. 保持进程存活，方便截图
 */
public class SparkUiDemo {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("Spark UI Demo")
                // local-cluster[worker 数, 每个 worker 的 core 数, 每个 worker 的内存 MB]
                // 这样 Executors 页签里能看到多个真正的 Executor，而不是只有 driver
                .master("local-cluster[3,2,1024]")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.ui.port", "4040")
                .config("spark.driver.memory", "2g")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        rddShuffleAndCache(spark);
        skewedAggregation(spark);
        dataFrameJoin(spark);
        sqlAggregation(spark);

        System.out.println("==> 批处理作业执行完毕，Spark UI: http://localhost:4040");
        System.out.println("==> 窗口期 1：截图 Jobs / Stages / Storage / Environment / Executors / SQL ...");
        Thread.sleep(240_000L);

        // 再启动流：保证 Streaming 页签存在，并让前面截完的 Jobs 列表保持干净
        streaming(spark);
        System.out.println("==> 窗口期 2：流已启动，截图 Streaming 页签 ...");
        Thread.sleep(60 * 60 * 1000L);
        spark.stop();
    }

    /** 1) RDD shuffle + persist：制造 Jobs / Stages / Storage 三个页签的数据。 */
    private static void rddShuffleAndCache(SparkSession spark) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Integer> nums = new ArrayList<>();
        for (int i = 0; i < 200000; i++) {
            nums.add(i);
        }
        JavaRDD<Integer> base = sc.parallelize(nums, 8);

        // 一次 map + reduceByKey，必然产生 shuffle（SortShuffleManager）
        JavaPairRDD<String, Integer> pairs = base.mapToPair(
                (PairFunction<Integer, String, Integer>) v -> new Tuple2<>("key-" + (v % 200), v));
        JavaPairRDD<String, Integer> reduced = pairs.reduceByKey(Integer::sum);
        System.out.println("== no-cache count : " + reduced.count());

        // persist 之后 Storage 页签会出现这条 RDD
        reduced.persist(StorageLevel.MEMORY_AND_DISK());
        System.out.println("== cached count   : " + reduced.count());
        System.out.println("== cached sample  : " + reduced.take(3));

        // 再来一个 groupByKey，产生第二次 shuffle
        JavaPairRDD<String, Iterable<Integer>> grouped = pairs.groupByKey();
        System.out.println("== groupByKey size: " + grouped.count());

        // 一个 sortByKey，产生 range partition 的 shuffle
        System.out.println("== sortByKey top3 : " + reduced.sortByKey().take(3));
    }

    /**
     * 2) 热点 Key：一个 Key 占了 90% 的数据。
     *
     * 为了让 Web UI 上的差异足够明显，数据量放大到 800 万条，
     * 并且只分 8 个 reduce 分区 —— 热点分区会明显比其他分区慢。
     */
    private static void skewedAggregation(SparkSession spark) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final int total = 4_000_000;
        JavaRDD<Integer> idx = sc.parallelize(
                java.util.stream.IntStream.range(0, total).boxed().collect(java.util.stream.Collectors.toList()), 8);

        // 在 Executor 侧生成数据，避免 Driver 内存压力
        JavaPairRDD<String, Integer> pairs = idx.mapPartitionsToPair(iter -> {
            java.util.Random random = new java.util.Random();
            java.util.List<Tuple2<String, Integer>> out = new java.util.ArrayList<>();
            while (iter.hasNext()) {
                iter.next();
                // 90% 的记录落到同一个热点 Key
                String key = random.nextInt(10) == 0
                        ? "user-" + random.nextInt(500)
                        : "hot-key";
                out.add(new Tuple2<>(key, random.nextInt(100)));
            }
            return out.iterator();
        });

        // 用 groupByKey：热点 Key 的全部 value 都会被拉到同一个 reduce 分区，
        // 这是最能体现「数据倾斜」的写法（也是为什么生产上不推荐 groupByKey）。
        JavaPairRDD<String, Iterable<Integer>> grouped = pairs.groupByKey(8);

        // 遍历每个 Key 的所有 value，热点分区的耗时会明显高于其它分区
        // 用 collect() 而不是 take()：take 只会跑前几个分区，
        // collect 才会让 8 个 reduce 分区全部执行，Web UI 上才看得到完整的 Task 分布
        System.out.println("== skewed keys    : " + grouped
                .mapValues(values -> {
                    long count = 0L;
                    for (Integer value : values) {
                        count += 1;
                    }
                    return count;
                })
                .collect().size());
    }

    /** 3) DataFrame join：制造 SQL 页签里的 join DAG。 */
    private static void dataFrameJoin(SparkSession spark) {
        List<Row> orders = new ArrayList<>();
        List<Row> users = new ArrayList<>();
        Random random = new Random(7);
        for (int i = 0; i < 100000; i++) {
            orders.add(RowFactory.create(i, random.nextInt(1000), random.nextInt(500) + 1));
        }
        for (int i = 1; i <= 1000; i++) {
            users.add(RowFactory.create(i, "city-" + (i % 20)));
        }
        StructType orderSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("order_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("user_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("amount", DataTypes.IntegerType, false)});
        StructType userSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("user_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("city", DataTypes.StringType, false)});

        Dataset<Row> orderDf = spark.createDataFrame(orders, orderSchema);
        Dataset<Row> userDf = spark.createDataFrame(users, userSchema);
        orderDf.createOrReplaceTempView("orders");
        userDf.createOrReplaceTempView("users");

        // 一次 join + 聚合：SQL 页签会出现带 Exchange 的 DAG
        Dataset<Row> joined = orderDf.join(userDf, "user_id");
        joined.cache();
        System.out.println("== join count     : " + joined.count());

        Dataset<Row> byCity = joined.groupBy("city").sum("amount");
        byCity.orderBy(byCity.col("sum(amount)").desc()).show(5);
    }

    /** 4) 纯 SQL：分组聚合 + 窗口函数，SQL 页签能看到完整计划。 */
    private static void sqlAggregation(SparkSession spark) {
        Dataset<Row> top = spark.sql(
                "SELECT city, COUNT(*) AS cnt, SUM(amount) AS total "
                        + "FROM orders o JOIN users u ON o.user_id = u.user_id "
                        + "GROUP BY city ORDER BY total DESC LIMIT 10");
        top.show();

        Dataset<Row> ranked = spark.sql(
                "SELECT user_id, amount, "
                        + "ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn "
                        + "FROM orders");
        ranked.createOrReplaceTempView("ranked_orders");
        spark.sql("SELECT * FROM ranked_orders WHERE rn <= 3").show(5);

        Dataset<Row> rolling = spark.sql(
                "SELECT user_id, amount, "
                        + "SUM(amount) OVER (PARTITION BY user_id ORDER BY order_id "
                        + "ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_sum "
                        + "FROM orders");
        rolling.show(5);
    }

    /** 5) Structured Streaming：制造 Streaming 页签的数据。 */
    private static void streaming(SparkSession spark) throws Exception {
        Dataset<Row> rate = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", "200")
                .load();

        Dataset<Row> counted = rate
                .withColumn("bucket", rate.col("value").mod(10))
                .groupBy("bucket")
                .count();

        StreamingQuery query = counted.writeStream()
                .outputMode("complete")
                .format("memory")
                .queryName("rate_bucket_count")
                // 60 秒一批：既能攒出几个微批，又不会把 Jobs 列表刷屏
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("60 seconds"))
                .start();

        // 等两个微批跑出来，查询保持 running 状态（Streaming 页签需要活跃查询）
        Thread.sleep(140_000L);
        System.out.println("== streaming status: " + query.status().message());
        System.out.println("== streaming rows  : " + spark.sql("SELECT * FROM rate_bucket_count").count());
    }
}
