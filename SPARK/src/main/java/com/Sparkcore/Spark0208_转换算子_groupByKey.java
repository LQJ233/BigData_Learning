package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

//groupByKey：按 key 分组，相同 key 的所有 value 聚合到一个 Iterable 集合中
//注意：groupByKey 会全量 shuffle（不做分区内预聚合），数据量大时性能较差，优先使用 reduceByKey
public class Spark0208_转换算子_groupByKey
{
    public static void main(String[] args)
    {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码：创建 KV 对 RDD
        JavaPairRDD<String, Integer> kvRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("hello", 1),
                new Tuple2<>("world", 1),
                new Tuple2<>("hello", 1)), 2);

        // 按 key 分组：相同 key 的 value 聚合到 Iterable 集合
        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = kvRDD.groupByKey();
        groupByKeyRDD.collect().forEach(System.out::println);
        // 结果：(hello,[1, 1]) (world,[1])

        // 4. 关闭 sc
        sc.stop();
    }
}
