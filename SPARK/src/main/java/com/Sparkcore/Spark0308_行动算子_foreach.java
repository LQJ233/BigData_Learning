package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

//遍历 RDD 中每一个元素, 对每一个元素都执行指定的函数
public class Spark0308_行动算子_foreach
{
    public static void main(String[] args)
    {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码：创建 RDD
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

        // 对每个元素执行函数（在 Executor 端执行，打印顺序不保证）
        rdd.foreach(x -> System.out.println(x));

        // 4. 关闭 sc
        sc.stop();
    }
}
