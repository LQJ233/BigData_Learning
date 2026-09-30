package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

//返回 RDD 中元素的个数
public class Spark0301_行动算子_count
{
    public static void main(String[] args)
    {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码：创建 RDD
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

        long count = rdd.count();
        System.out.println(count); // 4

        // 4. 关闭 sc
        sc.stop();
    }
}
