package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

//以数组 Array 的形式返回数据集的所有元素。
//
//请注意，collect 操作会将整个数据集加载到驱动程序的内存中，因此在处理大型数据集时要谨慎使用。如果数据集非常大，可能会导致内存溢出。
//
public class Spark0302_行动算子_collect
{
    public static void main(String[] args)
    {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码：创建 RDD
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        // 将分布式数据收集回 Driver 端，形成 Java List
        List<Integer> list = rdd.collect();
        list.forEach(System.out::println);

        // 4. 关闭 sc
        sc.stop();
    }
}
