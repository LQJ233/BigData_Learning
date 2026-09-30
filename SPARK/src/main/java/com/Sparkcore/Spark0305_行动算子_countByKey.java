package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

//统计每种 key 的个数
public class Spark0305_行动算子_countByKey
{
    public static void main(String[] args)
    {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码：创建 KV 对 RDD
        JavaPairRDD<String, Integer> kvRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("a", 3)), 2);

        // 统计每种 key 出现的次数，返回 Map
        Map<String, Long> countMap = kvRDD.countByKey();
        System.out.println(countMap); // {a=2, b=1}

        // 4. 关闭 sc
        sc.stop();
    }
}
