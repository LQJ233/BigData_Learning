package org.SparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import java.io.Serializable;
import static org.apache.spark.sql.functions.udaf;




//UDAF：输入多行，返回一行。通常和 groupBy 一起使用，
// 如果直接使用 UDAF 函数，默认将所有的数据合并在一起。


public class UDAF
{
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("My Spark Application")
                .config("spark.driver.host","localhost")
                .master("local[*]")
                .getOrCreate();



//3. 编写代码
        spark.read().json("Spark/src/main/data/user.json").createOrReplaceTempView("user");
        spark.udf().register("avgAge",udaf(new MyAvg(),Encoders.LONG()));
        spark.sql("select avgAge(age) newAge from user").show();
//4. 关闭 sparkSession
        spark.close();
    }
    public static class Buffer implements Serializable
    {
        private Long sum;
        private Long count;
        public Buffer() {
        }
        public Buffer(Long sum, Long count) {
            this.sum = sum;
            this.count = count;
        }


        public Long getSum() {
            return sum;
        }
        public void setSum(Long sum) {
            this.sum = sum;
        }
        public Long getCount() {
            return count;
        }
        public void setCount(Long count) {
            this.count = count;
        }
    }
    public static class MyAvg extends Aggregator<Long,Buffer,Double>
    {
        @Override
        public Buffer zero() {
            return new Buffer(0L,0L);
        }
        @Override
        public Buffer reduce(Buffer b, Long a) {
            b.setSum(b.getSum() + a);
            b.setCount(b.getCount() + 1);
            return b;
        }
        @Override
        public Buffer merge(Buffer b1, Buffer b2) {
            b1.setSum(b1.getSum() + b2.getSum());
            b1.setCount(b1.getCount() + b2.getCount());
            return b1;
        }
        @Override
        public Double finish(Buffer reduction) {
            return reduction.getSum().doubleValue() / reduction.getCount();
        }
        @Override
        public Encoder<Buffer> bufferEncoder() {
// 可以用 kryo 进行优化
            return Encoders.kryo(Buffer.class);
        }
        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }

    }
}
