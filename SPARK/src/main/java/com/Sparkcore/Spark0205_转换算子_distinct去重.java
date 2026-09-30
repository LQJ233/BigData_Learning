package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Spark0205_转换算子_distinct去重
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]") // 使用所有可用的本地线程
                .set("spark.executor.memory", "4g") // 设置每个executor的内存为4GB
                .set("spark.sql.shuffle.partitions", "2"); // 设置shuffle分区数


        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> integerJavaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
    // 底层使用分布式分组去重 所有速度比较慢,但是不会 OOM
        JavaRDD<Integer> distinct = integerJavaRDD.distinct();
        distinct. collect().forEach(System.out::println);
    // 4. 关闭 sc
        sc.stop();

    }
}
