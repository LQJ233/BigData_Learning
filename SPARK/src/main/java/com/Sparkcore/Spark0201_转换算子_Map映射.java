package com.Sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.List;


public class Spark0201_转换算子_Map映射
{
    public static class Spark02_转换算子
    {

        //Spark的方法主要是两大类，转换（Transformer）和行动（Action）

        public static void main(String[] args)
        {
            // 1.创建配置对象
            SparkConf conf = new SparkConf()
                    .setMaster("local[*]")
                    .setAppName("sparkCore")
                    .set("spark.driver.host","localhost");//不加这句会报错;;
//            // 2. 创建 sparkContext
            JavaSparkContext sc = new JavaSparkContext(conf);
            //以上写法主要用于老版本，不支持dataframe，不支持dataset，不支持sparkSQL，不支持sparkStreaming

            // 创建 SparkSession,这才是最推荐的写法
            SparkSession spark = SparkSession.builder()
                    .config("spark.driver.host","localhost")//不加这句会报错
                    .appName("My Spark Application")
                    .master("local[*]")  // 使用本地所有可用的核心进行计算

                    .getOrCreate();

//RDD的操作
            JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4));


            //lambda表达式写法: 参数列表 -> 主体
            //参数列表类似函数的形参
            //主体可以是一个表达式或语句块

            //单个参数的lambda表达式
            JavaRDD<Integer> Numbers1 = numbers.map(x -> x + x);

            //多个参数的lambda表达式（map 只接受一个参数，此处仅作语法演示，实际编译不通过）
//            JavaRDD<Integer> Numbers2 = numbers.map((x, y) -> x * y);
            //带有类型声明的参数
            JavaRDD<Integer> Numbers3 = numbers.map((Integer x) -> x / 2);
            //具有复杂逻辑的lambda表达式
            JavaRDD<Integer> Numbers4 = numbers.map(x -> {
                int sum = x + 1;
                return sum * 2;
            });



//DataSet操作
            // 从集合创建 DataFrame：一行一列，列名为 name
            Dataset<Row> df = spark.createDataFrame(
                    Arrays.asList(RowFactory.create("Alice"), RowFactory.create("Bob")),
                    DataTypes.createStructType(new StructField[]{
                            DataTypes.createStructField("name", DataTypes.StringType, true)
                    }));

// 定义一个 MapFunction 来修改每一行
            Encoder<String> stringEncoder = Encoders.STRING();
            //Encoders.STRING() 是一个静态工厂方法，它返回一个 Encoder<String> 类型的实例，这个实例可以将任何类型的对象编码成字符串。
            // 这个 Encoder 实例可以用于 Dataset 或 DataFrame 的操作中，例如在 map 操作中，将每一行数据转换为字符串
            Dataset<String> resultDF = df.map(
                    (MapFunction<Row, String>) row -> "Value: " + row.getString(0),
                    stringEncoder
            );//这个函数接受一个 Row 对象作为输入，并返回一个字符串。
            // 将 Row 对象的第一个字段（假设是字符串类型）取出，然后在前面添加 "Value: " 前缀，形成一个新的字符串。
            //stringEncoder将函数返回的字符串编码成 Spark 可以处理的数据格式



            List<Integer> collected = Numbers1.collect();





            for (Integer num : collected) {
                System.out.println(num);
            }

//            // 输出数据的函数写法
//            mapRDD1.collect().forEach(a -> System.out.println(a));
//            mapRDD1.collect().forEach(System.out::println);



            // 4. 关闭 sc
            sc.stop();
            spark.stop();
        }

    }

}