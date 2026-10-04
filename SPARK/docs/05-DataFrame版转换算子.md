# 05 · DataFrame 版转换算子

> 对应章节：03 章是 **RDD 版**转换算子（函数式 + lambda），本章是 **DataFrame / Dataset 版**转换算子（列式 + `Column` 表达式）。
>
> **核心区别**：DataFrame 的转换算子操作的是「列」，返回新的 `Dataset<Row>`，且全程是**惰性**的（同 RDD），要遇到行动算子才真正执行。

![DataFrame / SQL 背后的 Catalyst 优化链路](./images/spark-sql-pipeline.svg)

## 5.1 先建立认知：列式 API vs 函数式 API

| | RDD（03 章） | DataFrame / Dataset（本章） |
|---|---|---|
| 操作对象 | 元素 `T` | 列 `Column` / 行 `Row` |
| 传入什么 | `lambda` 函数 | `Column` 表达式（如 `col("age").gt(3)`）或 SQL 字符串 |
| 有无 Schema | 无 | 有 |
| 返回类型 | `JavaRDD<T>` | `Dataset<Row>`（或强类型 `Dataset<T>`） |
| 是否惰性 | 是 | 是 |

> **记忆**：RDD 是"对着每个元素写 lambda"；DataFrame 是"对着列名写表达式"。DataFrame 转换几乎不需要写 lambda，而是用 `select` / `filter` / `withColumn` + `functions` 里的列函数。

---

## 5.2 转换算子速查表

| 算子 | 作用 | 是否惰性 |
|---|---|---|
| `select` | 选择若干列（可对列做运算/别名） | 是 |
| `withColumn` | 新增或替换一列 | 是 |
| `withColumnRenamed` | 列重命名 | 是 |
| `filter` / `where` | 按条件过滤行 | 是 |
| `drop` | 删除若干列 | 是 |
| `distinct` / `dropDuplicates` | 去重 | 是 |
| `sort` / `orderBy` | 排序 | 是 |
| `groupBy` + `agg` | 分组聚合 | 是 |
| `join` | 连接两个 DataFrame | 是 |
| `union` / `unionByName` | 上下拼接 | 是 |
| `limit` | 限制行数 | 是 |
| `explode` | 一行拆多行（对应 RDD flatMap） | 是 |

---

## 5.3 核心转换算子详解

> 示例数据：`Spark/src/main/data/user.json`（`name` / `age` 两列）

```java
Dataset<Row> df = spark.read().json("Spark/src/main/data/user.json");
```

### ① select：选列 + 列运算

```java
// 选单列 / 多列
df.select("name");
df.select("name", "age");

// 对列做运算并起别名
df.select(col("name"), col("age").plus(1).as("age_next_year"));
// SQL 写法等价：df.selectExpr("name", "age + 1 as age_next_year")
```

### ② withColumn：新增 / 替换一列

```java
// 新增一列：年龄是否成年
df.withColumn("isAdult", col("age").geq(18));
// 替换一列：age 加 10
df.withColumn("age", col("age").plus(10));
```

### ③ filter / where：过滤行

```java
// 三种等价写法
df.filter(col("age").gt(18));     // 列表达式
df.where("age > 18");             // SQL 字符串
df.filter("age > 18");            // filter 也支持字符串
```

> ⚠️ **与 RDD 最大的不同**：`filter` 里传的是**列表达式或 SQL 字符串**，不是 `lambda`。传 `row -> ...` 会编译不过。

### ④ drop / withColumnRenamed：删列、改列名

```java
df.drop("age");                                  // 删除 age 列
df.withColumnRenamed("name", "userName");        // 列重命名
```

### ⑤ distinct / dropDuplicates：去重

```java
df.distinct();                        // 整行去重
df.dropDuplicates("name");            // 按指定列去重（保留第一个）
```

### ⑥ sort / orderBy：排序

```java
df.sort(col("age").desc());           // 按 age 降序
df.orderBy(col("age").asc(), col("name"));  // 多列排序
```

### ⑦ groupBy + agg：分组聚合（对应 RDD 的 groupByKey/reduceByKey）

```java
import static org.apache.spark.sql.functions.*;

// 按 name 分组，求每组的年龄总和、人数
df.groupBy("name")
  .agg(sum("age").as("total_age"), count("age").as("cnt"));

// 常用聚合函数：sum / count / avg / max / min
df.groupBy("name").agg(avg("age"));
```

> **注意**：`groupBy` 返回的是 `RelationalGroupedDataset`，**必须接 `agg`（或 `count`/`sum` 等快捷方法）**才能得到 DataFrame，不能像 RDD 那样 `groupBy` 后直接拿到集合。

### ⑧ join：连接

```java
Dataset<Row> df1 = ...; // 例如 user.json
Dataset<Row> df2 = ...; // 例如另一个表

df1.join(df2, df1.col("name").equalTo(df2.col("name")));   // 内连接，指定连接列
df1.join(df2, "name");                                      // 同名列简写
// 指定连接类型：join(df2, 条件, "left" / "right" / "outer")
```

### ⑨ union / unionByName：上下拼接

```java
df1.union(df2);           // 按列位置拼接（要求列数一致）
df1.unionByName(df2);     // 按列名拼接（列顺序可不同）
```

### ⑩ explode：一行拆多行（对应 RDD flatMap）

```java
// 假设某列是数组 [1,2,3]，explode 后每个元素一行
df.select(explode(col("tags")));
```

---

## 5.4 与 RDD 转换算子的对照（速查）

| RDD（03 章） | DataFrame / Dataset（本章） |
|---|---|
| `map` | `select` / `withColumn` + 列函数 |
| `flatMap` | `explode` / `split` |
| `filter` | `filter` / `where`（传列表达式） |
| `groupBy` | `groupBy(...).agg(...)` |
| `groupByKey` / `reduceByKey` | `groupBy(...).agg(sum/avg/...)` |
| `distinct` | `distinct` / `dropDuplicates` |
| `sortBy` / `sortByKey` | `sort` / `orderBy` |
| `mapValues` | `withColumn` |
| `join` | `join`（指定连接列） |
| `union` | `union` / `unionByName` |

---

## 5.5 强类型 Dataset 的转换（补充）

强类型 `Dataset<T>`（如 `Dataset<Person>`）里，`map` / `filter` 又能用 lambda 了，但**必须传 Encoder**：

```java
Dataset<Person> ds = df.as(Encoders.bean(Person.class));

// 强类型 Dataset 可以用 map（返回类型需要 Encoder）
Dataset<String> names = ds.map(p -> p.getName(), Encoders.STRING());

// 强类型 Dataset 的 filter 也可以用 lambda
Dataset<Person> adults = ds.filter(p -> p.getAge() >= 18);
```

> **总结**：`Dataset<Row>`（DataFrame）用列表达式；强类型 `Dataset<T>` 能用 lambda（要 Encoder），最接近 RDD 的写法。日常开发优先用 DataFrame（列式，优化器能优化）；需要编译期类型安全时再转强类型 Dataset。
