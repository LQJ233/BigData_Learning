# 02 · Spark 数据导入与数据结构（RDD / DataFrame / Dataset）



## 2.1 三大数据结构：RDD / DataFrame / Dataset



| 对比项 | RDD | DataFrame | Dataset |
|---|---|---|---|
| 是否有 Schema（表结构） | 无 | **有**（列名 + 类型） | **有** |
| 类型安全 | **强类型**（编译期检查） | 弱类型（`Row` 运行时才知道类型） | **强类型**（编译期检查） |
| 优化引擎 | 无（直接算子链） | Catalyst 优化器 + Tungsten 执行 | 同 DataFrame |
| Java 中的类型 | `JavaRDD<T>` / `JavaPairRDD<K,V>` | `Dataset<Row>` | `Dataset<T>` |
| 适用场景 | 底层、非结构化、需要精细控制 | 结构化数据、SQL/DSL 操作 | 结构化 + 强类型编程 |
| 序列化 | Java/Kryo 序列化 | Tungsten 二进制（列式） | Encoder（更高效） |



> ⚠️ **易混淆点**：`DataFrame` 和 `Dataset` 在 Java 里是同一个类 `org.apache.spark.sql.Dataset`，区别只在于泛型参数——`Dataset<Row>` 就是 DataFrame，`Dataset<Person>` 就是强类型 Dataset。源码里的 `Dataset<Row> df` 等价于 DataFrame。

---

## 2.2 JavaSparkContext 与SparkSession

导入数据时，**用哪个入口对象，就得到哪种数据结构**：

| 入口对象 | 导入方法 | 得到的数据结构 |
|---|---|---|
| `JavaSparkContext`（老） | `parallelize` / `textFile` 等 | `RDD` |
| `SparkSession`（新） | `spark.read().xxx()` | `DataFrame`、`Dataset` |

> 关键区别：`JavaSparkContext` 只能得到 RDD；`SparkSession.read()` 读进来默认是 DataFrame（`Dataset<Row>`）。要得到强类型 Dataset，需要额外用 Encoder 转换（见 2.6）。

---

## 2.3 导入为 RDD（JavaSparkContext）

> 对应源码：[Spark01_数据导入.java](../src/main/java/com/Sparkcore/Spark01_数据导入.java)

RDD 是 Spark 最基础的数据结构，导入方式主要两种：**从集合**、**从文件**。

### ① 从集合创建 RDD（parallelize / parallelizePairs）

```java
// 创建 JavaSparkContext（老写法，只能操作 RDD）
SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
JavaSparkContext sc = new JavaSparkContext(conf);

// ---- 普通集合 → JavaRDD（parallelize）
JavaRDD<String> stringRDD = sc.parallelize(
        Arrays.asList("hello", "spark", "hello", "spark", "hello"), 2);
// 第 2 个参数 2：分成 2 个分区

// ---- 键值对集合 → JavaPairRDD（parallelizePairs）
JavaPairRDD<String, Integer> kvRDD = sc.parallelizePairs(Arrays.asList(
        new Tuple2<>("hello", 1),
        new Tuple2<>("world", 1),
        new Tuple2<>("hello", 1)));

sc.stop();
```

### ② 从文本文件创建 RDD（textFile）

```java
// textFile：从文本文件/目录读取，每一行作为 RDD 的一个元素
JavaRDD<String> lineRDD = sc.textFile("input");

// 收集回 Driver 端并转成 Java List
List<String> result = lineRDD.collect();
```

### RDD 分区机制

**知识点①：集合分区的整数除机制（左闭右开）**

以 5 个元素、2 个分区为例，分区计算采用**整数除、左闭右开**规则：

```
0 分区 => start = 0 * 5 / 2 = 0，end = 1 * 5 / 2 = 2   => 元素 1, 2
1 分区 => start = 1 * 5 / 2 = 2，end = 2 * 5 / 2 = 5   => 元素 3, 4, 5
```

**知识点②：文件分区的计算公式**

`textFile` 读取文件时，具体分区个数需要经过公式计算：

```
1. 获取文件的总长度 totalSize
2. 计算平均长度 goalSize = totalSize / numSplits（numSplits 为填写的分区数）
3. 获取块大小 blockSize = 128M（HDFS 默认块大小）
4. 计算切分大小 splitSize = Math.max(minSize, Math.min(goalSize, blockSize))
5. 使用 splitSize 按照 1.1 倍原则切分整个文件，得到几个分区就是几个分区
```

> **速记**：实际开发中只需比较「文件总大小 / 填写分区数」与「块大小 128M」，谁小就用谁切分。

**知识点③：数据会分配到哪个分区**

- 如果切分位置位于一行的**中间**，会在当前分区**读完一整行数据**（不会把一行拆到两个分区）。
- 例如 4 行数据分成 4 个分区时：0 分区读 1、2 行，1 分区读 3 行，2 分区读 4 行，3 分区为空。

**知识点④：saveAsTextFile 输出格式**

Spark 会根据 RDD 的分区数，将每个分区的数据保存为一个文件。例如 2 个分区会生成：

```
output/
├── part-00000   （第一个分区的数据）
└── part-00001   （第二个分区的数据）
```

文件内容是纯文本，每行对应 RDD 中的一个元素。

---

## 2.4 导入为 DataFrame（SparkSession.read）

> DataFrame（`Dataset<Row>`）是日常开发主力，通过 `SparkSession.read()` 能直接读 JSON、CSV、JDBC 数据库等结构化数据源。

### ① 导入 JSON → DataFrame

> 对应源码：[导入json.java](../src/main/java/org/SparkSQL/导入json.java)

```java
SparkSession spark = SparkSession.builder()
        .config("spark.driver.host", "localhost")
        .appName("My Spark Application")
        .master("local[*]")
        .getOrCreate();

// 读取 JSON 为 DataFrame（Dataset<Row>）
Dataset<Row> df = spark.read().json("Spark/src/main/data/user.json");

// 注册临时视图后可用 SQL 查询
df.createOrReplaceTempView("user");
df = spark.sql("select * from user");
df.show();

spark.stop();
```

**user.json 数据预览**：

```json
{"age":20,"name":"qiaofeng"}
{"age":19,"name":"xuzhu"}
{"age":18,"name":"duanyu"}
```

> **知识点**：JSON 每一行是一个独立的对象，Spark 会**自动推断 Schema**（列名 `name`、`age`，类型 String/Long）。项目里还有 `iris.json`（鸢尾花，150 行）可替换体验。

### ② 导入 CSV → DataFrame（可指定分隔符、标题行）

> 对应源码：[Spark00_Spark基本格式.java](../src/main/java/com/Sparkcore/Spark00_Spark基本格式.java)

```java
// 读取 CSV，指定分隔符和标题行
Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", "true")   // 第一行是标题（列名）
        .option("sep", ";")         // 分号分隔（对应 Tariff Calculations.csv）
        .load("Spark/src/main/data/Tariff Calculations.csv");

df.show();
```

> **知识点**：CSV 常用选项——`header`（是否有标题行）、`sep`/`delimiter`（分隔符）、`inferSchema`（是否自动推断类型）。项目内其他分隔符数据（tab 分隔）见 2.7。

### ③ 导入 MySQL（JDBC）→ DataFrame

> 对应源码：[test1.java](../src/main/java/SparkStream/test1.java)（流式批次内读取）

```java
Dataset<Row> empDF = spark.read()
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/test01")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "emp")          // 表名
        .option("user", "root")
        .option("password", System.getenv("MYSQL_PASSWORD"))
        .load();

empDF.show(false); // false：不截断列
```

> **知识点**：JDBC 导入需要 MySQL 驱动依赖（`mysql-connector-java`，已在 Spark/pom.xml 配置）；密码已改为环境变量 `MYSQL_PASSWORD` 读取，运行前需 `export MYSQL_PASSWORD=你的密码`。

### ④ 导入 Parquet → DataFrame（列式格式，Spark 官方推荐）

**Parquet 是什么**：一种**列式存储**的二进制文件格式，是 Spark 最推荐的数据格式。特点：

| 特性 | 说明 |
|---|---|
| 列式存储 | 按列而非按行存放，读某几列时只需读对应列，跳过无关数据 |
| 高压缩比 | 同列数据类型一致，压缩效率远高于行式（如 CSV/JSON） |
| 自带 Schema | 元信息内嵌在文件中，读取时无需再推断或指定 |
| 谓词下推 | 只读取需要的列和行，大幅减少 IO |
| 跨语言 | Hive / Impala / Spark 等生态通用 |

```java
// 读取 Parquet（无需指定格式，Spark 默认就是 Parquet）
Dataset<Row> parquetDF = spark.read().parquet("output/user.parquet");

// 写入 Parquet
df.write().parquet("output/user.parquet");
// 也可用 spark.read().load("path") 或 df.write().save("path")，默认格式就是 parquet
```


### ⑤ Schema 的查看与手动指定

Spark 读结构化数据时会给 DataFrame 推断一个 **Schema**（列名 + 类型）。Schema 是 DataFrame/Dataset 区别于 RDD 的核心，掌握它的查看与手动指定是实战必备技能。

**1. 查看 Schema（printSchema）**

```java
Dataset<Row> df = spark.read().json("Spark/src/main/data/user.json");
df.printSchema();
// 输出：
// root
//  |-- age: long (nullable = true)
//  |-- name: string (nullable = true)
```

**2. 自动推断 vs 手动指定**

读 JSON/CSV 时 Spark 默认**自动推断 Schema**（扫描数据猜类型）。自动推断有两个问题：**慢**（大文件要额外扫一遍数据）、**可能猜错**（如把纯数字的 ID 猜成 long）。生产上通常**手动指定 Schema**：

```java
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

// 手动构建 Schema：name 为字符串、age 为长整型，都允许为空(nullable)
StructType schema = DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.LongType, true)
});

// 用指定 Schema 读取，跳过自动推断
Dataset<Row> df = spark.read()
        .schema(schema)            // 手动指定 Schema
        .json("Spark/src/main/data/user.json");

df.printSchema();
// root
//  |-- name: string (nullable = true)
//  |-- age: long (nullable = true)
```

> **知识点**：
> - `StructType` = 多个 `StructField` 的组合；`StructField(列名, 类型, 是否可为空)`。
> - 类型用 `DataTypes.xxx` 指定（`StringType` / `LongType` / `IntegerType` / `DoubleType` 等）。
> - **何时必须手动指定**：数据量大（自动推断慢）、需要精确控制列类型、数据里有脏数据会被猜错类型时。

---

## 2.5 导入为强类型 Dataset（需 Encoder）

Spark 读进来的默认是 DataFrame（`Dataset<Row>`，弱类型）。要得到**强类型 Dataset**（如 `Dataset<Person>`），需要：
1. 定义一个 POJO 类（`Person`）。
2. 用 `as(Encoders.bean(Person.class))` 把 `Dataset<Row>` 转成 `Dataset<Person>`。

```java
// 1. POJO 类（必须有 getter/setter 或 public 字段）
public static class Person {
    private String name;
    private long age;
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public long getAge() { return age; }
    public void setAge(long age) { this.age = age; }
}

// 2. 读 JSON 为 Dataset<Row>，再转强类型 Dataset<Person>
Dataset<Row> df = spark.read().json("Spark/src/main/data/user.json");

Dataset<Person> ds = df.as(Encoders.bean(Person.class));
// 现在 ds 是强类型：ds.map(person -> person.getName(), Encoders.STRING()) 可直接拿到 Person 对象
```

> **知识点**：`Encoders.bean(Class)` 把 Row 按字段名映射到 POJO。强类型 Dataset 的优势是**编译期就能发现类型错误**，缺点是写起来比 DataFrame 繁琐。

---

## 2.6 三种结构的互相转换

| 转换 | 写法 | 说明 |
|---|---|---|
| RDD → DataFrame | `rdd.toDF()` 或 `spark.createDataFrame(rdd, Schema)` | 需要 Schema（列名/类型） |
| RDD → Dataset | 先 `toDF()` 再 `as(Encoder)` | 强类型转换 |
| DataFrame → RDD | `df.javaRDD()` | 拿回 `JavaRDD<Row>` |
| DataFrame → Dataset | `df.as(Encoders.bean(Class))` | 弱类型转强类型 |
| Dataset → DataFrame | `ds.toDF()` | 强类型转回 `Dataset<Row>` |

```java
// 完整示例：RDD → DataFrame → 强类型 Dataset
JavaRDD<Person> personRDD = sc.parallelize(Arrays.asList(person1, person2));
Dataset<Row> df = spark.createDataFrame(personRDD, Person.class); // RDD → DataFrame
Dataset<Person> ds = df.as(Encoders.bean(Person.class));           // DataFrame → 强类型 Dataset
JavaRDD<Row> backToRDD = df.javaRDD();                             // DataFrame → RDD
```

---

## 2.7 数据源与导入结构速查表

| 数据源 | 入口 | 示例写法 | 得到结构 |
|---|---|---|---|
| 本地集合 | `JavaSparkContext` | `sc.parallelize(list, 2)` | `JavaRDD` |
| 键值对集合 | `JavaSparkContext` | `sc.parallelizePairs(kvList)` | `JavaPairRDD` |
| 文本文件 | `JavaSparkContext` | `sc.textFile("input")` | `JavaRDD<String>` |
| JSON 文件 | `SparkSession` | `spark.read().json("user.json")` | `Dataset<Row>`（DataFrame） |
| CSV 文件 | `SparkSession` | `spark.read().format("csv").option(...).load()` | `Dataset<Row>`（DataFrame） |
| Parquet 文件 | `SparkSession` | `spark.read().parquet("path")` | `Dataset<Row>`（DataFrame） |
| MySQL 表 | `SparkSession` | `spark.read().format("jdbc").option(...).load()` | `Dataset<Row>`（DataFrame） |
| 强类型对象 | `SparkSession` | `df.as(Encoders.bean(Person.class))` | `Dataset<Person>` |

### 项目内可用数据文件

| 文件 | 内容 | 分隔符 | 适合导入方式 |
|---|---|---|---|
| `user.json` | 用户姓名/年龄（6 行） | JSON | `spark.read().json()` |
| `iris.json` | 鸢尾花数据集（150 行） | JSON | `spark.read().json()` |
| `product_inf0.txt` | 商品信息（100 行） | tab（`\t`） | `sc.textFile()` 或 CSV 指定 `delimiter="\t"` |
| `city_info` | 城市信息（26 行） | tab（`\t`） | `sc.textFile()` |
| `user_visit_action` | 用户访问行为（18 万行） | tab（`\t`） | `sc.textFile()`（大文件演示） |

> **提示**：tab 分隔的文本文件，用 `sc.textFile()` 读成 RDD 后需自行 `split("\t")` 解析；也可用 `spark.read().format("csv").option("delimiter", "\t")` 直接读成 DataFrame。

---

## 2.8 本章小结

1. **RDD / DataFrame / Dataset** 是三代抽象：RDD 无 Schema、类型安全；DataFrame 有 Schema、弱类型；Dataset 有 Schema 且强类型。
2. **入口决定结构**：`JavaSparkContext` → RDD；`SparkSession.read()` → DataFrame（`Dataset<Row>`）。
3. **导入 RDD**：集合用 `parallelize` / `parallelizePairs`，文件用 `textFile`。
4. **导入 DataFrame**：`spark.read().json() / .format("csv") / .parquet() / .format("jdbc")`。
5. **导入强类型 Dataset**：DataFrame 再 `as(Encoders.bean(Class))`。
6. **Schema**：用 `printSchema()` 查看，用 `StructType` + `DataTypes` 手动指定（生产推荐）。
7. **Parquet**：列式存储、自带 Schema、高压缩，是 Spark 官方推荐的落盘格式。
