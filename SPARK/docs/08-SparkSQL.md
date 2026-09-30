# 08 · SparkSQL：临时视图、UDF 与 UDAF

> 对应源码：[导入json.java](../src/main/java/org/SparkSQL/导入json.java)、[UDF.java](../src/main/java/org/SparkSQL/UDF.java)、[UDAF.java](../src/main/java/org/SparkSQL/UDAF.java)
> 示例数据：`Spark/src/main/data/user.json`（用户姓名/年龄）

## 8.1 导入 JSON 并创建临时视图

> 对应源码：[导入json.java](../src/main/java/org/SparkSQL/导入json.java)

**作用**：用 `spark.read().json(...)` 读取 JSON 文件为 DataFrame（Dataset\<Row\>），再通过 `createOrReplaceTempView` 注册为临时视图，之后即可用 SQL 查询。

```java
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

        // 读取 JSON 为 DataFrame
        Dataset<Row> df = spark.read().json("Spark/src/main/data/user.json");
        // 路径已统一为项目内相对路径（基于项目根目录运行）

        // 将 df 转换为临时视图
        df.createOrReplaceTempView("user");
        // 固定格式记住就行。视图只能查询，不能增删改，即不要去直接动原来那个文件

        // 用 SQL 查询临时视图
        df = spark.sql("select * from user");

        // df.show(); // 打印查询结果
        spark.stop();
    }
}
```

> **知识点**：
> - `createOrReplaceTempView`：注册一个**会话级临时视图**，只在当前 SparkSession 内有效；同名视图会替换。
> - 临时视图是只读的（查询用），不会修改原始数据文件。
> - 源码中还有一段被注释的 `iris.json` 读取示例（`flower` 表），可自行放开体验多张表。

**user.json 数据预览**：

```json
{"age":20,"name":"qiaofeng"}
{"age":19,"name":"xuzhu"}
{"age":18,"name":"duanyu"}
{"age":22,"name":"qiaofeng"}
{"age":11,"name":"xuzhu"}
{"age":12,"name":"duanyu"}
```

## 8.2 UDF：自定义函数

> 对应源码：[UDF.java](../src/main/java/org/SparkSQL/UDF.java)

**作用**：UDF（User Defined Function）——**一行进，一行出**。用于在 SQL 中调用自定义的普通函数，如给名字加后缀。

```java
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

        // 读取 JSON 并注册临时表
        Dataset<Row> lineRDD = spark.read().json("Spark/src/main/data/user.json");
        lineRDD.createOrReplaceTempView("user");

        // ---- 写法1：UDF1 匿名内部类
        // udf 函数接受两个参数：一个是实现了 UDF1 接口的匿名内部类实例，另一个是返回值的数据类型
        UserDefinedFunction addName = udf(new UDF1<String, String>()
        {
            @Override
            public String call(String s) throws Exception
            {
                return s + " 大侠";
            }
        }, DataTypes.StringType); // 表示返回的是一个字符串

        // 注册 UDF，使其可以在 SQL 查询中使用
        // 第一个参数 addName 是注册的名字，方便在 SQL 中使用；第二个是上面定义的 UDF1
        spark.udf().register("addName", addName);

        // 使用注册的 UDF 进行 SQL 查询并显示结果
        spark.sql("select addName(name) newName from user") // 对 name 列应用 addName，结果命名为 newName
                .show();

        // ---- 写法2：lambda 表达式定义 UDF（更简洁）
        spark.udf().register("addName1",
                (UDF1<String, String>) name -> name + " 大 侠 ",
                DataTypes.StringType);

        spark.close();
    }
}
```

**运行结果（示意）**：

```
+-----------+
|    newName|
+-----------+
| qiaofeng 大侠|
|   xuzhu 大侠|
...
+-----------+
```

> **知识点**：
> - 匿名类写法：`udf(new UDF1<输入类型, 输出类型>() { call(...) }, 返回类型)`。
> - lambda 写法：`spark.udf().register("函数名", (UDF1<输入,输出>) 参数 -> 表达式, 返回类型)`。
> - 注册后才能用 `spark.sql("select 函数名(列) ...")` 调用；UDF1 = 1 个入参，UDF2/UDF3 以此类推。

## 8.3 UDAF：自定义聚合函数

> 对应源码：[UDAF.java](../src/main/java/org/SparkSQL/UDAF.java)

**作用**：UDAF（User Defined Aggregate Function）——**输入多行，返回一行**，通常和 `groupBy` 一起使用；如果直接使用 UDAF，默认将**所有数据合并在一起**。

以「求年龄平均值 MyAvg」为例，需要继承 `Aggregator<输入类型, 缓冲类型, 输出类型>` 并实现 5 个方法：

```java
package org.SparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

import static org.apache.spark.sql.functions.udaf;

public class UDAF
{
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("My Spark Application")
                .config("spark.driver.host","localhost")
                .master("local[*]")
                .getOrCreate();

        // 读取 JSON、注册临时表
        spark.read().json("Spark/src/main/data/user.json").createOrReplaceTempView("user");

        // 注册 UDAF：udaf(聚合器实例, 输入类型 Encoder)
        spark.udf().register("avgAge", udaf(new MyAvg(), Encoders.LONG()));

        // SQL 中使用：对所有行的 age 求平均
        spark.sql("select avgAge(age) newAge from user").show();

        spark.close();
    }

    // 缓冲类：保存聚合过程的中间状态（必须可序列化）
    public static class Buffer implements Serializable
    {
        private Long sum;    // 累加和
        private Long count;  // 元素个数

        public Buffer() {}
        public Buffer(Long sum, Long count) {
            this.sum = sum;
            this.count = count;
        }
        // getter / setter 省略（源码中有完整实现）
        public Long getSum() { return sum; }
        public void setSum(Long sum) { this.sum = sum; }
        public Long getCount() { return count; }
        public void setCount(Long count) { this.count = count; }
    }

    // 聚合器：Aggregator<输入 Long, 缓冲 Buffer, 输出 Double>
    public static class MyAvg extends Aggregator<Long, Buffer, Double>
    {
        // 1. 初始化缓冲（零值）
        @Override
        public Buffer zero() {
            return new Buffer(0L, 0L);
        }

        // 2. 分区内：把一条数据归约进缓冲
        @Override
        public Buffer reduce(Buffer b, Long a) {
            b.setSum(b.getSum() + a);
            b.setCount(b.getCount() + 1);
            return b;
        }

        // 3. 合并：合并两个分区的缓冲
        @Override
        public Buffer merge(Buffer b1, Buffer b2) {
            b1.setSum(b1.getSum() + b2.getSum());
            b1.setCount(b1.getCount() + b2.getCount());
            return b1;
        }

        // 4. 收尾：从缓冲计算出最终结果
        @Override
        public Double finish(Buffer reduction) {
            return reduction.getSum().doubleValue() / reduction.getCount();
        }

        // 5. 缓冲的编码器（可以用 kryo 进行优化）
        @Override
        public Encoder<Buffer> bufferEncoder() {
            return Encoders.kryo(Buffer.class);
        }

        // 6. 输出结果的编码器
        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }
}
```

**运行结果（示意）**：user.json 6 条数据的平均年龄 = (20+19+18+22+11+12) / 6 ≈ **17**

> **知识点**：
> - `Aggregator<IN, BUF, OUT>` 三个泛型：输入类型、缓冲类型、输出类型。
> - 4 个核心方法对应聚合的执行流程：`zero`（初始化）→ `reduce`（分区内逐条归约）→ `merge`（跨分区合并）→ `finish`（计算结果）。
> - `bufferEncoder` / `outputEncoder`：Spark 需要把缓冲和结果序列化传输，用 `Encoders.kryo` 可优化序列化性能。
> - 注册时第二个参数 `Encoders.LONG()` 表示**输入类型**的编码器（这里输入是 age 列，Long 类型）。

## 8.4 UDF vs UDAF 对比

| 对比项 | UDF | UDAF |
|---|---|---|
| 输入输出 | 一行进，一行出 | 多行进，一行出 |
| 典型场景 | 字符串处理、字段加工 | 求和、平均、自定义统计指标 |
| 接口 | `UDF1<IN, OUT>` / lambda | 继承 `Aggregator<IN, BUF, OUT>` |
| 实现复杂度 | 低（一个 call 方法） | 高（zero/reduce/merge/finish + 编码器） |
| 是否结合聚合 | 否 | 通常与 groupBy / 全表聚合结合 |
