package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

//mapValues：只对 KV 对中的 value 应用函数，key 保持不变
//与 map 的区别：map 需要同时处理 key 和 value，mapValues 只处理 value，更简洁高效
public class Spark0207_转换算子_mapVaules
{
    public static void main(String[] args)
    {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        // 2. 创建 sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码：从集合创建 KV 对 RDD（parallelizePairs）
        JavaPairRDD<String, Integer> kvRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("c", 3)), 2);

        // 只对 value 进行操作：每个 value 加 10，key 保持不变
        JavaPairRDD<String, Integer> mapValuesRDD = kvRDD.mapValues(v -> v + 10);
        mapValuesRDD.collect().forEach(System.out::println);
        // 结果：(a,11) (b,12) (c,13)

        // 4. 关闭 sc
        sc.stop();
    }
}
