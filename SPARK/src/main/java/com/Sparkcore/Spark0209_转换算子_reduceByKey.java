package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

//reduceByKey：按 key 聚合，相同 key 的 value 应用归约函数（如求和、求最大值）
//性能优于 groupByKey：先在每个分区内做局部聚合（map 端预聚合），再 shuffle 全局聚合，传输的数据量更小
public class Spark0209_转换算子_reduceByKey
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

        // 按 key 求和（Integer::sum 等价于 (x, y) -> x + y）
        JavaPairRDD<String, Integer> reduceByKeyRDD = kvRDD.reduceByKey(Integer::sum);
        reduceByKeyRDD.collect().forEach(System.out::println);
        // 结果：(hello,2) (world,1)

        // 4. 关闭 sc
        sc.stop();
    }
}
