package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark01_数据导入
{
	public static void main(String[] args)
    {

        // 1.创建配置对象,我们可以定义如何运行 Spark 应用，包括集群模式、应用名称、内存、核心数等。
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
        //setMaster() 方法用于设置 Spark 集群的 master URL，即告诉 Spark 该应用程序在哪个环境下运行。
        //"local[*]"用于指示 Spark 在本地模式下运行。"local" 表示 Spark 将在本地机器上运行，而不是在集群上运行。[*] 表示 Spark 会使用本地机器的所有 CPU 核心。
        //.setAppName("sparkCore")用于设置一个名称，这个名称会出现在 Spark UI 中，以便于你在查看或监控时识别出这个应用。

        // 2. 创建sparkContext,专门用于初始化 Spark 的核心上下文。它相当于 Spark 的入口点，用于连接到 Spark 集群、执行操作、创建RDD等。
        JavaSparkContext sc = new JavaSparkContext(conf);


        // 3. 创建RDD


        //----从集合中创建RDD
        JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("hello", "org/SparkSQL", "hello", "org/SparkSQL", "hello"),2);
        //JavaRDD<String>这是一个包含 String 类型元素的 RDD。
        //通过 sc，你可以调用 Spark 的方法来创建 RDD。
        //parallelize(...) 是 SparkContext 中的一个方法，用于将本地的 Java 集合（如 List、Array）并行化为 RDD。
        // 这意味着它会将数据拆分为多个分区，并分发到不同的工作节点中进行并行处理。
        //Arrays.asList(...) 是 Java 的一个方法，它将传入的多个元素组合成一个 List（列表）。
        //2表示将 RDD 分成 2 个分区，每个分区可以并行处理。

        // 数据分区的情况
        // 0 => 1,2 1 => 3,4,5
        // 利用整数除机制 左闭右开
        // 0 => start 0*5/2 end 1*5/2
        // 1 => start 1*5/2 end 2*5/2

        // 4.将 stringRDD 中的数据保存到 output 目录
        stringRDD.saveAsTextFile("output");
/*
Spark 会根据 RDD 的分区数，将每个分区的数据保存为一个文件。例如，如果 RDD 有 2 个分区（如之前的代码），那么 "output" 目录下可能会有两个文件：
part-00000（第一个分区的数据）
part-00001（第二个分区的数据）

输出格式：
每个分区文件中的数据是纯文本格式，每行对应 RDD 中的一个元素。
例如，假设 stringRDD 中的数据是 ["hello", "spark", "hello", "spark", "hello"]，存储的文件内容可能会类似这样：

文件 part-00000:
hello
spark
hello
文件 part-00001:
spark
hello
 */

        //----从外部文件创建RDD

        JavaRDD<String> lineRDD = sc.textFile("input");
        //textFile(...)：是 SparkContext 的一个方法，用于从文本文件或文件目录中读取数据，并将每一行数据转化为一个 RDD。
        //// 默认填写的最小分区数 2 和环境的核数取小的值 一般为 2
        List<String> result = lineRDD.collect();
        //将 RDD 中的数据从分布式环境收集回本地并转换为一个 Java 的 List 集合。后续可以for循环，打印这个列表


        // 具体的分区个数需要经过公式计算
        // 首先获取文件的总长度 totalSize
        // 计算平均长度 goalSize = totalSize / numSplits
        // 获取块大小 128M
        // 计算切分大小 splitSize = Math.max(minSize, Math.min(goalSize, blockSize));
        // 最后使用 splitSize 按照 1.1 倍原则切分整个文件 得到几个分区就是几个分区
        // 实际开发中 只需要看文件总大小 / 填写的分区数 和块大小比较 谁小拿谁进行切分

        // 数据会分配到哪个分区
        // 如果切分的位置位于一行的中间 会在当前分区读完一整行数据
        // 0 -> 1,2 1 -> 3 2 -> 4 3 -> 空



        // 4. 关闭sc
        sc.stop();
    }




}
