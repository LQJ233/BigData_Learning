# 06 · DataFrame 版行动算子

> 对应章节：04 章是 **RDD 版**行动算子，本章是 **DataFrame / Dataset 版**行动算子。
>
> **核心概念**（同 RDD）：行动算子会**触发真正的计算**（提交 Job）。DataFrame 的转换算子是惰性的，只有遇到行动算子才真正执行。

## 6.1 行动算子速查表

| 算子 | 作用 | 结果去向 |
|---|---|---|
| `show` | 打印前 N 行（最常用） | 打印到控制台 |
| `count` | 返回行数 | 返回 Driver |
| `collect` / `collectAsList` | 收集所有行 | 返回 Driver |
| `first` / `head` | 返回第一行 | 返回 Driver |
| `head(n)` / `take(n)` / `limit` | 返回前 n 行 | 返回 Driver |
| `describe` | 列统计（count/mean/stddev/min/max） | 返回 Driver |
| `printSchema` | 打印 Schema | 打印到控制台 |
| `foreach` / `foreachPartition` | 逐行 / 逐分区执行函数 | 分布式执行 |
| `write().xxx()` | 写出到文件系统 | 写入外部存储 |
| `isEmpty` | 判断是否为空 | 返回 Driver |

---

## 6.2 核心行动算子详解

> 示例数据：`Spark/src/main/data/user.json`（`name` / `age` 两列）

```java
Dataset<Row> df = spark.read().json("Spark/src/main/data/user.json");
```

### ① show：打印（最常用的行动算子）

```java
df.show();           // 默认打印前 20 行
df.show(5);          // 打印前 5 行
df.show(false);      // false：不截断列（长内容完整显示）
```

### ② count：返回行数

```java
long rowCount = df.count();   // 返回总行数
```

### ③ collect / collectAsList：收集所有行

```java
List<Row> rows = df.collectAsList();   // 收集为 List<Row>
for (Row row : rows) {
    System.out.println(row.getString(0));   // 按位置取列
    System.out.println(row.getAs("name"));  // 按列名取列
}
```

> ⚠️ **同 RDD 的 collect**：会把所有数据拉到 Driver 内存，大 DataFrame 慎用，可能 OOM。

### ④ first / head / take / limit：取前几行

```java
Row firstRow = df.first();      // 第一行
Row[] rows = df.head(3);        // 前 3 行（数组）
List<Row> rows2 = df.takeAsList(3);   // 前 3 行（List）
Dataset<Row> limited = df.limit(3);   // 前 3 行（仍是 DataFrame，惰性）
```

> **head/take vs limit**：`head(n)` / `take(n)` 是**行动算子**（返回数据）；`limit(n)` 是**转换算子**（返回 DataFrame，惰性），常配合后续行动。

### ⑤ describe：列统计（非常实用）

```java
df.describe("age").show();
// 输出 count / mean / stddev / min / max 等统计量
```

### ⑥ printSchema：打印表结构

```java
df.printSchema();
// root
//  |-- age: long (nullable = true)
//  |-- name: string (nullable = true)
```

### ⑦ foreach / foreachPartition：逐行 / 逐分区执行

```java
// foreach：每行执行一次（在 Executor 端，顺序不保证）
df.foreach(row -> System.out.println(row.getString(0)));

// foreachPartition：每分区执行一次（可复用连接，写数据库推荐）
df.foreachPartition(iter -> {
    Connection conn = getConnection();   // 每分区一个连接
    while (iter.hasNext()) {
        Row row = iter.next();
        conn.insert(row);
    }
    conn.close();
});
```

### ⑧ write：写出到外部存储（对应 RDD 的 saveAsTextFile）

```java
// 写成各种格式（都是行动，触发计算）
df.write().csv("output/csv");
df.write().json("output/json");
df.write().parquet("output/parquet");   // 默认格式，推荐
df.write().text("output/text");         // 仅单列时可用
```

> **saveAsTextFile vs write**：RDD 用 `saveAsTextFile`（只能写文本）；DataFrame 用 `write().format(...)`（可写 csv/json/parquet 等，更灵活）。

### ⑨ isEmpty：判断是否为空

```java
boolean empty = df.isEmpty();   // 常用条件判断
```

---

## 6.3 与 RDD 行动算子的对照（速查）

| RDD（04 章） | DataFrame / Dataset（本章） |
|---|---|
| `count` | `count()` |
| `collect` | `collect` / `collectAsList` |
| `first` | `first` / `head` |
| `take(n)` | `head(n)` / `takeAsList(n)` / `limit(n)` |
| `takeOrdered` / `top` | `orderBy(...).limit(n)`（排序 + 取前 n） |
| `reduce` / `fold` / `aggregate` | `agg(sum(...))` 等聚合函数 |
| `countByKey` / `countByValue` | `groupBy(...).count()` |
| `foreach` / `foreachPartition` | `foreach` / `foreachPartition`（用法几乎相同） |
| `saveAsTextFile` | `write().text()` / `.csv()` / `.parquet()` |
| `isEmpty` | `isEmpty()` |
| 无对应 | `show()` / `printSchema()` / `describe()`（DataFrame 独有，日常调试神器） |

---

## 6.4 行动算子按"结果去向"分类

| 结果去向 | 算子 | 特点 |
|---|---|---|
| **返回 Driver 端** | count / collect / first / head / take / describe / isEmpty | 结果传回 Driver，大数据量有 OOM 风险 |
| **打印到控制台** | show / printSchema | 调试用，show 最常用 |
| **写入外部存储** | write().xxx() / foreach / foreachPartition | 结果留在分布式侧，适合大数据 |

---

## 6.5 常见性能陷阱

1. **collect 大 DataFrame → OOM**：改 `limit`/`head` 后再收集，或 `write()` 分布式输出。
2. **show 不是"无害"操作**：show 也会触发计算，频繁调用会重复算（记得先 `cache`）。
3. **foreach 打印顺序不保证**：想按顺序看用 `collectAsList` 后遍历。
4. **写目录已存在会报错**：`write().csv("output")` 若目录已存在会抛异常，需先删或加 `.mode("overwrite")`。
5. **write 默认格式是 parquet**：`df.write().save("path")` 不写 format 时默认 parquet。
