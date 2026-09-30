package com.Sparkcore;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;


//groupBy 会存在 shuffle 过程
public class Spark0203_转换算子_groupBy分组
    // 需求：将 List 集合中的数据进行分组
{
    public static void main(String[] args)
    {
    // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
    // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);



    // 3. 编写代码
        JavaRDD<Integer> integerJavaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

    // 泛型为分组标记的类型
        JavaPairRDD<Integer, Iterable<Integer>> groupByRDD = integerJavaRDD.groupBy(new Function<Integer, Integer>()
        {
            @Override
            public Integer call(Integer v1) throws Exception
            {
                return v1 % 2;
            }
        });
        groupByRDD.collect().forEach(System.out::println);

    // 类型可以任意修改
        JavaPairRDD<Boolean, Iterable<Integer>> groupByRDD1 = integerJavaRDD.groupBy(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });
        groupByRDD1.collect().forEach(System.out::println);
        //collect将数据收集到 Driver 端，形成一个列表，forEach（）对列表到的列表中的每个元素调用println方法
        //Thread.sleep(600000);
    // 4. 关闭 sc
        sc.stop();
    }
}
