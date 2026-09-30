package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

//sortByKey：对 KV 对的 RDD 按 key 排序，默认升序（true 为升序，false 为降序）
//排序结果全局有序，同样涉及 shuffle
public class Spark0210_转换算子_sortByKey
{
    public static void main(String[] args)
    {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码：创建 KV 对 RDD
        JavaPairRDD<String, Integer> kvRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("b", 2),
                new Tuple2<>("a", 1),
                new Tuple2<>("c", 3)), 2);

        // 按 key 升序排序（参数 true 表示升序）
        JavaPairRDD<String, Integer> sortByKeyRDD = kvRDD.sortByKey(true);
        sortByKeyRDD.collect().forEach(System.out::println);
        // 结果：(a,1) (b,2) (c,3)

        // 4. 关闭 sc
        sc.stop();
    }
}
