package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

//返回一个由 RDD 的前 n 个元素组成的数组
public class Spark0304_行动算子_take
{
    public static void main(String[] args)
    {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码：创建 RDD
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        // 返回前 3 个元素
        List<Integer> top3 = rdd.take(3);
        System.out.println(top3); // [1, 2, 3]

        // 4. 关闭 sc
        sc.stop();
    }
}
