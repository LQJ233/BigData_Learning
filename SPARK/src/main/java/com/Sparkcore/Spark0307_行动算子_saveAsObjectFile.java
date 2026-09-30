package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

//用于将 RDD 中的元素序列化成对象，存储到文件中。
public class Spark0307_行动算子_saveAsObjectFile
{
    public static void main(String[] args)
    {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码：创建 RDD
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        // 保存为 Java 序列化对象文件
        rdd.saveAsObjectFile("output/saveAsObjectFile_out");

        // 通过 objectFile 读回
        JavaRDD<Integer> loaded = sc.objectFile("output/saveAsObjectFile_out");
        loaded.collect().forEach(System.out::println);

        // 4. 关闭 sc
        sc.stop();
    }
}
