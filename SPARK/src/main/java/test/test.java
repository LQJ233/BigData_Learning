package test;
import org.apache.commons.math3.stat.descriptive.summary.Product;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class test {
    public static void main(String[] args) {
        // 创建一个 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Read CSV Example")
                .config("spark.driver.host","localhost")
                .master("local[*]") // 如果是在本地运行，使用所有可用的核心
                .getOrCreate();

        // 指定CSV文件的路径
        String filePath = "Spark/src/main/data/Tariff Calculations.csv";

        // 读取CSV文件为DataFrame
        Dataset<Row> df = spark.read()
                .option("header", true) // 如果CSV文件有标题行
                .option("sep", ";") // 该数据文件为分号分隔
                .csv(filePath);
        // 打印DataFrame的结构信息
        //df.printSchema();

        // 显示DataFrame的内容，默认显示20行
        df.show();

        // 如果你想显示所有行，可以这样做：
        // df.show(false); // 不截断任何列
        // 或者指定显示的行数
        // df.show(100); // 显示100行

        // 停止SparkSession
        spark.stop();
    }
}
