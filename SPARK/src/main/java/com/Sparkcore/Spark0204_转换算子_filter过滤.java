package com.Sparkcore;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

//根据指定的条件来筛选RDD、DataFrame或Dataset中的元素。filter 操作会返回一个新的数据集，这个新数据集中只包含满足给定条件的元素。
//filter 接收一个函数作为参数，该函数定义了筛选的逻辑。这个函数会被应用到RDD中的每个元素上，只有当函数返回 true 时，对应的元素才会被保留在结果RDD中。
public class Spark0204_转换算子_filter过滤
{
    public static void main(String[] args) {
        // 初始化SparkContext
        JavaSparkContext sc = new JavaSparkContext("local", "Filter Example");

        // 创建一个简单的整数RDD
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // 使用filter筛选出所有大于3的元素
        JavaRDD<Integer> filteredRDD = rdd.filter(x -> x > 3);

        // 收集结果并打印
        System.out.println(filteredRDD.collect());

        // 关闭SparkContext
        sc.close();
    }
}
