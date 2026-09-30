package org.SparkSQL;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.udf;
public class UDF
{


    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("My Spark Application")
                .config("spark.driver.host","localhost")
                .master("local[*]")
                .getOrCreate();



//3. 编写代码
        Dataset<Row> lineRDD = spark.read().json("Spark/src/main/data/user.json");
        lineRDD.createOrReplaceTempView("user");

        //UDF：一行进入，一行出
        // 定义一个UDF：函数名为addName
        //udf 函数接受两个参数：一个是实现了 UDF1 接口的匿名内部类实例，另一个是返回值的数据类型
        UserDefinedFunction addName = udf(new UDF1<String, String>()
        {
            @Override
            public String call(String s) throws Exception
            {
                return s + " 大侠";
            }

        }, DataTypes.StringType);//表示返回的是一个字符串。



        // 注册 UDF，使其可以在 SQL 查询中使用
        spark.udf().register("addName",addName);//第一个addName是注册的名字，方便下面在sql中使用。第二个addName是上面定义的UDF1
        // 使用注册的 UDF 进行 SQL 查询，并显示结果
        spark.sql("select addName(name) newName from user")//从 user 表中选择 name 列，并使用 addName 函数对其进行处理，将处理结果命名为 newName。
                .show();

        // 使用 lambda 表达式定义 UDF 的另一种写法
        spark.udf().register("addName1",(UDF1<String,String>) name -> name + " 大 侠 ",DataTypes.StringType);


//4. 关闭 sparkSession
                spark.close();
    }
}
