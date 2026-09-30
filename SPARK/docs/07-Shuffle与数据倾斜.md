# 07 · Shuffle 与数据倾斜专题

> 前面 03/04 章反复提到"是否 shuffle"，本章系统讲透：**shuffle 是什么、为什么慢、数据倾斜怎么破**。这是性能优化和面试的核心专题。

---

## 7.1 Shuffle 是什么

**Shuffle** = 数据在集群中**跨节点重新分配**的过程。当某个算子需要把**相同 key 的数据聚合到一起**时（如 groupByKey、reduceByKey、join），不同节点上的数据必须通过网络传输到"属于该 key 的节点"，这个过程就是 shuffle。

```
    分区0：a, b, c         分区1：a, c         分区2：b, d
          │                    │                    │
          └──────────┬─────────┴─────────┬──────────┘
                     │    Shuffle（按 key 重新分发）  │
                     ▼                     ▼
              key=a 的所有数据        key=b 的所有数据
              聚到同一分区            聚到同一分区
```

> **一句话**：shuffle 就是把"分散在各节点的同 key 数据"搬到一起。搬数据 = 磁盘 IO + 网络传输，所以 shuffle 是分布式计算里**最昂贵**的操作。

---

## 7.2 宽依赖 vs 窄依赖（shuffle 的判定标准）

Spark 用「依赖关系」来判定是否需要 shuffle：

| 依赖类型 | 定义 | 是否 shuffle | 典型算子 |
|---|---|---|---|
| **窄依赖** | 父 RDD 的每个分区最多被**一个**子分区使用 | **否** | map、flatMap、filter、mapValues |
| **宽依赖** | 父 RDD 的每个分区会被**多个**子分区使用 | **是** | groupByKey、reduceByKey、join、distinct、sortBy |

```
窄依赖（无 shuffle）：                    宽依赖（有 shuffle）：
  父分区 → 子分区（一对一/多对一）          父分区 → 多个子分区（一对多）
  P0 ─────▶ C0                          P0 ──┬──▶ C0
  P1 ─────▶ C0                          P1 ──┼──▶ C0
                                        P2 ──┴──▶ C1
```

> **记忆**：子分区如果只"认领"一个父分区，就是窄依赖（无 shuffle）；如果子分区需要从**多个父分区**拿数据，就是宽依赖（有 shuffle）。

### 为什么宽依赖要切 Stage

一个 Job 内部按 shuffle 边界切成多个 **Stage**（阶段）：

```
Stage1（窄依赖）──shuffle──▶ Stage2（窄依赖）──shuffle──▶ Stage3
```

每个 Stage 内部是窄依赖，可以**流水线并行**；Stage 之间必须等上一阶段全部算完、数据 shuffle 到位，才能开始下一阶段。所以 **shuffle 次数越多，Stage 越多，性能越差**。

---

## 7.3 数据倾斜（Data Skew）

**数据倾斜** = shuffle 时某个 key 的数据量**远超其他 key**，导致这个 key 所在的 task 处理时间远长于其他 task，形成"木桶效应"——整体任务卡在最慢的那个 task 上。

### 典型场景

- `reduceByKey` / `groupByKey`：某个 key（如"热点商品"、"null 值"）数据特别多。
- `join`：两个表里某个 key 的记录数极不均衡。

### 数据倾斜的表象

```
task 1：处理 100 条，1 秒完成
task 2：处理 100 条，1 秒完成
task 3：处理 10,000,000 条，10 分钟还没完  ← 倾斜的 key，拖垮整个 Job
```

> **危害**：少数 task 拖慢整体、个别 executor OOM（数据都挤到少数节点）、资源利用率低。

---

## 7.4 数据倾斜的解决方案

按"从简到繁"排列，优先用前面的：

### 方案① 过滤掉倾斜的 key（最简单）

如果倾斜的 key 是脏数据（如 `null`、空字符串），直接过滤：

```java
// 过滤掉 null key
rdd.filter(t -> t._1 != null);
// 或 DataFrame：df.filter(col("key").isNotNull());
```

### 方案② 提高 shuffle 并行度

增加 shuffle 分区数，让数据分散到更多 task：

```java
// 增加 shuffle 分区数（默认 200）
spark.conf().set("spark.sql.shuffle.partitions", "2000");
// RDD 版：reduceByKey 传分区数
rdd.reduceByKey((x, y) -> x + y, 1000);
```

> **局限**：只是"稀释"了每个 task 的数据量，不能根治——倾斜的 key 还是会集中到同一个 task（同一个 key 的数据必然在一起）。

### 方案③ 两阶段聚合（加随机前缀，常用）

给倾斜的 key 加随机前缀打散，先局部聚合，再去前缀二次聚合：

```java
// 第一轮：key 加随机前缀，局部聚合（把大 key 打散）
JavaPairRDD<String, Integer> step1 = rdd
        .mapToPair(t -> new Tuple2<>(new Random().nextInt(10) + "_" + t._1, t._2))
        .reduceByKey(Integer::sum);

// 第二轮：去掉前缀，再次聚合
JavaPairRDD<String, Integer> result = step1
        .mapToPair(t -> new Tuple2<>(t._1.split("_", 2)[1], t._2))
        .reduceByKey(Integer::sum);
```

> **原理**：把 `hot_key` 拆成 `0_hot_key`、`1_hot_key`...`9_hot_key`，原本挤在一个 task 的数据分散到 10 个 task 先聚合，再汇总。

### 方案④ 大表 join 小表：广播（Broadcast）

如果 join 的一边是小表，把它**广播**到每个 executor，避免 shuffle：

```java
// DataFrame 版：广播小表
import static org.apache.spark.sql.functions.broadcast;

df1.join(broadcast(df2), "key");   // 小表 df2 广播，大表 df1 不用 shuffle
```

> **原理**：小表数据复制到每个节点内存，join 时直接本地匹配，完全消除 shuffle。**适用**：小表足够小（默认 <10MB 自动广播，可调 `spark.sql.autoBroadcastJoinThreshold`）。

### 方案⑤ 倾斜 key 单独处理

把倾斜的 key 和正常的 key 分开处理，再合并：

```java
// 1. 找出倾斜的 key（如数据量最大的 key）
// 2. 倾斜 key 单独做：加前缀打散后 join
// 3. 正常 key 正常 join
// 4. 结果 union 合并
```

> 适合倾斜 key 明确且数量少的场景，处理更精准，但代码复杂。

---

## 7.5 常见问题与调优口诀

### 高频面试题：groupByKey vs reduceByKey 为什么性能差这么多？

**reduceByKey 有 map 端预聚合（combiner）**：先在每个分区内对相同 key 局部聚合，shuffle 时只传输聚合后的少量数据；**groupByKey 不做预聚合**，把全量数据 shuffle。数据量一大，groupByKey 的网络和磁盘开销成倍增加。

```
reduceByKey：分区内先 sum → shuffle 传 (key, 局部和) → 再 sum   [数据量小]
groupByKey ：分区内不聚合 → shuffle 传 (key, 全部 value) → 再 sum [数据量大]
```

### 调优口诀

1. **能不用 shuffle 就不用**：`map`/`filter`/`mapValues` 优于 `groupByKey`。
2. **能预聚合就预聚合**：优先 `reduceByKey` / `aggregateByKey`，少用 `groupByKey`。
3. **小表 join 用广播**：彻底消除 shuffle。
4. **shuffle 分区数要调**：数据量大时调大 `spark.sql.shuffle.partitions`。
5. **观察 Spark UI**：看 Stage 里 task 的执行时间是否均衡，定位倾斜。

---

## 7.6 本章小结

1. **Shuffle** = 同 key 数据跨节点重新分配，是分布式计算最贵的操作。
2. **宽依赖（有 shuffle）vs 窄依赖（无 shuffle）**：看子分区是否依赖多个父分区。
3. **数据倾斜** = shuffle 时少数 key 数据量过大，拖垮整体。
4. **解决方案**（从简到繁）：过滤脏 key → 提高并行度 → 加前缀两阶段聚合 → 小表广播 → 倾斜 key 单独处理。
5. **核心心法**：减少 shuffle、增加预聚合、广播小表、调 shuffle 分区数。
