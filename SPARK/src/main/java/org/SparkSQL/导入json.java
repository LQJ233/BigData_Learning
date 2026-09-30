package org.SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class 导入json
{
    public static void main (String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("My Spark Application")
                .config("spark.driver.host","localhost")
                .master("local[*]")
                .getOrCreate();


        Dataset<Row> df=spark.read().json("Spark/src/main/data/user.json");



        df.createOrReplaceTempView("user");//固定格式记住就行，将df数据集转换为临时视图，试图只能查询，不能增删改查，即不要去直接动原来那个文件

//        Dataset<Row> df1=spark.read().json("Spark/src/main/data/iris.json");
//        df1.createOrReplaceTempView("flower");
//        df1.show();


        df=spark.sql("select * from user");

        //df.show();
        spark.stop();

    }
}
