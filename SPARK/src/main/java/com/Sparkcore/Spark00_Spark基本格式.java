package com.Sparkcore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Spark00_Spark基本格式
{
    public static void main(String[] args)
    {
//        // 1.创建配置对象,我们可以定义如何运行 Spark 应用，包括集群模式、应用名称、内存、核心数等。
//        SparkConf conf = new SparkConf()
//                .setMaster("local[*]")
//                .setAppName("sparkCore")
//                .set("spark.driver.host","localhost");//不加这句会报错;
//        //setMaster() 方法用于设置 Spark 集群的 master URL，即告诉 Spark 该应用程序在哪个环境下运行。
//        //"local[*]"用于指示 Spark 在本地模式下运行。"local" 表示 Spark 将在本地机器上运行，而不是在集群上运行。[*] 表示 Spark 会使用本地机器的所有 CPU 核心。
//        //.setAppName("sparkCore")用于设置一个名称，这个名称会出现在 Spark UI 中 。
//
//        // 2. 创建sparkContext,专门用于初始化 Spark 的核心上下文。它相当于 Spark 的入口点，用于连接到 Spark 集群、执行操作、创建RDD等。
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        //以上写法主要用于老版本，不支持dataframe，不支持dataset，不支持sparkSQL，不支持sparkStreaming

        SparkSession spark = SparkSession
                .builder()
                .config("spark.driver.host","localhost")//不加这句会因为ip变动而报错报错
                .appName("My Spark Application")
                .master("local[*]")  // 使用本地所有可用的核心进行计算
                .getOrCreate(); //创建或获取一个 SparkSession 对象。

        //导入数据为dataSet,每行封装为一个row对象1
        Dataset<Row> df = spark
                .read().
                format("csv")//指定读取的是csv数据
                .option("header", "true") // 假设数据文件有标题行
                .option("sep", ";") // 该数据文件为分号分隔（Tariff Calculations.csv）
                .load("Spark/src/main/data/Tariff Calculations.csv");


        // 显示数据的前几行
        df.show();



        spark.stop();
    }
}
