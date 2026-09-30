# 07 · Shuffle 与数据倾斜专题

> 前面 03/04 章反复提到"是否 shuffle"，本章系统讲透：**shuffle 的底层机制、数据倾斜的定位与根治、以及按业务场景对症下药的完整方案手册**。这是性能优化和面试的核心专题。

---

## 7.1 Shuffle 是什么

**Shuffle** = 数据在集群中**跨节点重新分配**的过程。当某个算子需要把**相同 key 的数据聚合到一起**时（如 groupByKey、reduceByKey、join），不同节点上的数据必须通过磁盘 + 网络传输到"属于该 key 的节点"，这个过程就是 shuffle。

```
    分区0：a, b, c         分区1：a, c         分区2：b, d
          │                    │                    │
          └──────────┬─────────┴─────────┬──────────┘
                     │    Shuffle（按 key 重新分发）  │
                     ▼                     ▼
              key=a 的所有数据        key=b 的所有数据
              聚到同一分区            聚到同一分区
```

> **一句话**：shuffle 就是把"分散在各节点的同 key 数据"搬到一起。搬数据 = 磁盘 IO + 序列化 + 网络传输，所以 shuffle 是分布式计算里**最昂贵**的操作。

### 常见的会产生 shuffle 的算子

| 类别 | 算子 / 语法 |
|---|---|
| 聚合类 | `groupByKey`、`reduceByKey`、`aggregateByKey`、`combineByKey` |
| 分组类 | `groupBy`、SQL 中的 `GROUP BY` |
| 关联类 | `join`、`cogroup`、SQL 中的 `JOIN` |
| 去重排序类 | `distinct`、`sortBy`、`sortByKey`、SQL 中的 `ORDER BY` |
| 重分区类 | `repartition`、`coalesce(缩减也可能不 shuffle)`、`partitionBy` |

### 宽依赖 vs 窄依赖（shuffle 的判定标准）

| 依赖类型 | 定义 | 是否 shuffle | 典型算子 |
|---|---|---|---|
| **窄依赖** | 父 RDD 的每个分区最多被**一个**子分区使用 | **否** | map、flatMap、filter、mapValues |
| **宽依赖** | 父 RDD 的每个分区会被**多个**子分区使用 | **是** | groupByKey、reduceByKey、join、distinct、sortBy |

Job 按 shuffle 边界切分成多个 **Stage**：Stage 内部窄依赖可以流水线并行，Stage 之间必须等上一阶段 shuffle 到位才能开始。**shuffle 次数越多，Stage 越多，性能越差**。

---

## 7.2 ShuffleManager 的演进与运行原理

Spark 历史上先后使用过两种 ShuffleManager，理解它们的差异，就理解了"shuffle 为什么慢、慢在哪"。

> **本项目 Spark 3.3.1 说明**：HashShuffleManager 从 Spark 2.0 起已被移除，3.x 只保留 SortShuffleManager。但理解 Hash → Sort 的演进过程，是理解 shuffle 文件数量问题的最佳路径。

### 7.2.1 未优化的 HashShuffleManager（历史机制）

每个 task 为**下游 stage 的每一个 task** 单独创建一个磁盘文件，相同 key 的数据写入同一个文件：

![未经优化的HashShuffleManager](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/08f3b596.png)

**文件数量公式**：`每个 task 创建的文件数 = 下一个 stage 的 task 数量`

举例：当前 stage 有 50 个 task（10 个 executor、每个 5 个 task），下游 stage 有 100 个 task → 每个 executor 创建 500 个文件，**全集群 5000 个**文件。海量小文件带来惊人的磁盘 IO 和句柄开销。

### 7.2.2 consolidate 优化后的 HashShuffleManager（历史机制）

开启 `spark.shuffle.consolidateFiles=true` 后，引入 **shuffleFileGroup** 复用机制：同一批并行执行的 task 复用同一组文件，下一批 task 继续往里追加：

![优化后的HashShuffleManager](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/3f3c6a82.png)

**文件数量公式**：`每个 executor 的文件数 = CPU core 数 × 下游 task 数`

同上场景：每个 executor 100 个文件，全集群 **1000 个**（从 5000 → 1000）。但文件数量依然随并行度线性增长，治标不治本。

### 7.2.3 SortShuffleManager：普通运行机制（Spark 2.0+ 唯一机制）

![SortShuffleManager普通运行机制](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/b6d1140d.png)

运行过程四步：

1. 数据先写入**内存数据结构**：聚合类算子（reduceByKey 等）用 **PartitionedPairBuffer/Map**（边聚合边写），join 类普通算子用 **Array**。
2. 内存数据达到阈值后**溢写磁盘**，溢写前**按 key 排序**，分批写入（默认每批 1 万条，经 `BufferedOutputStream` 缓冲，减少磁盘 IO 次数）。
3. 多次溢写产生多个临时文件，最终 **merge 成一个磁盘文件**。
4. 额外生成一份**索引文件**，标识下游各 task 的数据在文件中的 offset 区间。

**文件数量公式**：`每个 task 只有 1 个数据文件 + 1 个索引文件`

同上场景：每个 executor 只有 **5 个**文件，全集群 **50 个**（5000 → 1000 → 50）。

### 7.2.4 SortShuffleManager：bypass 运行机制

**触发条件**（两个同时满足）：

1. shuffle map task 数量 **小于** `spark.shuffle.sort.bypassMergeThreshold`（默认 200）；
2. **不是聚合类** shuffle 算子（如 join、groupByKey 这类不需要 map 端聚合的算子）。

![SortShuffleManager bypass机制](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/35ec4dc8.png)

运行过程：为每个下游 task 创建一个临时文件按 hash 写入，最后合并成一个文件 + 索引文件。与普通机制的本质区别：**不进行排序**，省掉排序开销——磁盘写机制与老的 HashShuffleManager 一致，只是最后多一步合并。

### 文件数量演进总结

| 机制 | 文件总数（50 task × 10 executor × 100 下游 task） | 排序 |
|---|---|---|
| 未优化 Hash | 5000 | 否 |
| consolidate Hash | 1000 | 否 |
| Sort 普通 | 50 | 是 |
| Sort bypass | 50 | **否** |

---

## 7.3 数据倾斜：现象、原理与定位

### 7.3.1 数据倾斜的两种典型现象

1. **绝大多数 task 很快，个别 task 极慢**：比如 1000 个 task 里 997 个 1 分钟完成，剩下两三个跑了一两个小时——作业整体进度被少数 task 拖死。
2. **原本正常的作业突然 OOM**：某个 task 处理的数据量暴增导致内存溢出（注意：OOM 也可能是代码 bug 或脏数据，需用下面的方法确认）。

### 7.3.2 发生原理

shuffle 时，相同的 key 必须被拉取到同一个 task 处理。如果某个 key 的数据量远超其他 key，这个 key 所在的 task 就会成为瓶颈——**整个作业的运行时间由最慢的那个 task 决定**（木桶效应）。

![数据倾斜发生原理](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/b6cbcfd9.png)

如上图：`hello` 这个 key 在三个节点上共有 7 条数据，全部拉到同一个 task；`world`、`you` 各只有 1 条。第一个 task 的运行时间可能是另外两个的 7 倍。真实业务里热点 key（爆款商品、超级大 V、null 值）动辄百万条，差距是指数级的。

### 7.3.3 如何定位导致倾斜的代码

**第一步：在 Web UI 找到倾斜的 Stage。** 打开 Spark Web UI（见 10 章），进入 Stages 页，看每个 stage 内 task 的运行时间和数据量：

![WebUI定位数据倾斜](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/f34568b1.png)

如果绝大多数 task 处理几百 KB、个别 task 处理几 MB 甚至几十 MB（差 10 倍以上），即可确认数据倾斜。

**第二步：根据 stage 划分原理倒推代码。** 代码中**每出现一个 shuffle 类算子（或 SQL 中的 group by / join），就是一条 stage 边界**。从倾斜的 stage 往前数 shuffle 边界，就能定位到是哪一行代码：

```java
// 例：wordCount 的 stage 划分
JavaRDD<String> lines = sc.textFile("hdfs://...");   // ┐
JavaRDD<String> words = lines.flatMap(...);           // ├─ stage 0（到 shuffle write 为止）
JavaPairRDD<String, Integer> pairs = words.mapToPair(...);
JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum);  // ← shuffle 边界
counts.collect();                                     // ┘ stage 1（shuffle read + 聚合）
```

**第三步（可选）：查看 key 的分布。** 对倾斜的 RDD 采样统计，确认到底是哪些 key 在捣乱：

```java
// 采样 10% 的数据，统计每个 key 的数量
JavaPairRDD<String, Integer> sampled = pairs.sample(false, 0.1);
Map<String, Long> keyCounts = sampled.countByKey();
keyCounts.forEach((k, v) -> System.out.println(k + " -> " + v));
```

---

## 7.4 数据倾斜的通用解决方案

按"从简到繁"排列，**优先用前面的**：

### 方案一：源头预处理（Hive ETL）

**适用**：倾斜源自 Hive 表本身数据不均匀，且业务频繁对同一批表做分析。
**思路**：用 Hive ETL 提前按 key 聚合或提前 join 好中间表，Spark 直接读结果，不再执行 shuffle 算子。
**代价**：倾斜被"提前"到了 ETL 阶段，治标不治本，但换来 Spark 作业的大幅提速（交互式分析场景实测可提升数倍）。

### 方案二：过滤少数导致倾斜的 key

**适用**：倾斜 key 只有少数几个，且这些 key 对结果无意义（脏数据、测试账号、爬虫流量）。

```java
// RDD：直接 filter
rdd.filter(t -> t._1 != null && !t._1.isEmpty());
// DataFrame / SQL：where 过滤
df.filter(col("key").isNotNull());
spark.sql("SELECT ... FROM t WHERE key IS NOT NULL AND key != ''");
```

> 若不确定哪些 key 倾斜，可动态采样后过滤数据量最大的 key（见 7.3.3 第三步）。

### 方案三：提高 shuffle 并行度

**适用**：直面倾斜时**优先尝试**的最简单手段。

![提高shuffle并行度](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/ce6545e2.png)

```java
// RDD 层面：给 shuffle 算子传分区参数
rdd.reduceByKey((x, y) -> x + y, 1000);
// SQL 层面：调大 shuffle 分区数（默认 200，很多场景偏小）
spark.conf().set("spark.sql.shuffle.partitions", "2000");
```

**原理**：增加 shuffle read task 数量，让原本挤在一个 task 的多个 key 分散开。
**局限**：只是缓解不是根治——**同一个 key 的数据永远会落在同一个 task**，极端倾斜（单 key 百万条）时调多少分区都没用。

### 方案四：两阶段聚合（加盐打散，聚合类首选）

**适用**：`reduceByKey`、`aggregateByKey` 等**聚合类**算子或 SQL `GROUP BY` 产生的倾斜。

![两阶段聚合原理](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/ed298b30.png)

四步走：**加盐 → 局部聚合 → 去盐 → 全局聚合**：

```java
// ① 每个 key 打上 0~9 的随机前缀
JavaPairRDD<String, Integer> prefixed = rdd.mapToPair(t ->
        new Tuple2<>(new Random().nextInt(10) + "_" + t._1, t._2));

// ② 局部聚合：hot_key 被打散成 0_hot_key ~ 9_hot_key，分散到 10 个 task
JavaPairRDD<String, Integer> localAggr = prefixed.reduceByKey(Integer::sum);

// ③ 去掉前缀
JavaPairRDD<String, Integer> removed = localAggr.mapToPair(t ->
        new Tuple2<>(t._1.split("_", 2)[1], t._2));

// ④ 全局聚合：此时每个 key 的数据量已缩小为原来的 1/10
JavaPairRDD<String, Integer> result = removed.reduceByKey(Integer::sum);
```

> **原理**：`(hello,1)×4` → 加盐后 `(1_hello,1)(1_hello,1)(2_hello,1)(2_hello,1)` → 局部聚合 `(1_hello,2)(2_hello,2)` → 去盐 `(hello,2)(hello,2)` → 全局聚合 `(hello,4)`。对聚合类倾斜通常有数倍提升；**局限**：只适用于聚合类，join 类要用方案五/六/七。

### 方案五：reduce join 转 map join（小表广播）

**适用**：join 两边**一大一小**（小表几百 MB 到一两 GB）。

![reduce join转map join](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/af80b7cb.png)

普通 join（reduce join）两边都要 shuffle；广播小表后，join 在 map 端本地完成，**完全不发生 shuffle**：

```java
// DataFrame 写法（最常用）
import static org.apache.spark.sql.functions.broadcast;
df1.join(broadcast(df2), "key");   // 小表 df2 广播到每个 executor

// RDD 写法：collect 小表 → broadcast → map 端比对
List<Tuple2<Long, Row>> small = smallRdd.collect();           // 拉到 Driver
Broadcast<List<Tuple2<Long, Row>>> bc = sc.broadcast(small);  // 广播
JavaPairRDD<Long, String> joined = bigRdd.mapToPair(t -> {
    // 在算子内从广播变量取小表数据比对（小表 key 唯一时适用）
    ...
});
```

**注意**：小表会常驻 Driver 和每个 Executor 的内存，广播超过 10GB 的表可能 OOM。SQL 下可用 `spark.sql.autoBroadcastJoinThreshold`（默认 10MB）控制自动广播阈值，超小表 Spark 会自动广播。

### 方案六：采样倾斜 key 并分拆 join

**适用**：两个**大表** join（无法广播），但倾斜 key 只有少数几个。

![采样倾斜key并分拆join](https://awps-assets.meituan.net/mit-x/blog-images-bundle-2016/61c7dabc.png)

思路：把倾斜 key 拆出来单独处理，正常 key 正常 join：

```java
// ① 采样找出倾斜 key（如数据量最大的 userid）
JavaPairRDD<String, Integer> sampled = rdd1.sample(false, 0.1);
String skewedKey = /* 统计后取 top1 */;

// ② 表1 拆分：倾斜 key 数据（打 0~99 随机前缀） + 正常 key 数据
// ③ 表2 拆分：倾斜 key 数据膨胀 100 倍（每条复制 100 份，打上 0~99 前缀） + 正常 key 数据
// ④ 倾斜部分 join：打散后的 key 分散到 100 个 task
// ⑤ 正常部分照常 join
// ⑥ 两次结果 union
```

> **与方案七的区别**：只对**少数倾斜 key** 扩容，内存开销小；倾斜 key 成千上万个时不适用。

### 方案七：随机前缀 + 整体扩容 join（最后手段）

**适用**：**大量** key 都倾斜的大表 join，方案六拆不动时的兜底。

```java
int n = 100;
// 表1：每条数据打上 0~n 的随机前缀
// 表2：整体膨胀 n 倍，每条数据复制 n 份、分别打上 0~n 前缀
// join 后去掉前缀恢复原 key
```

**代价**：对整个 RDD 扩容 n 倍，内存要求高，且更多是缓解而非根治。实测一个严重倾斜的 join 从 60 分钟优化到 10 分钟左右。

### 方案八：组合使用

复杂场景没有银弹：典型组合是"**方案一/二 先过滤预处理 → 方案三 提并行度兜底 → 聚合类用方案四 → join 类按表大小选方案五/六/七**"。

---

## 7.5 按业务场景对症下药

通用方案讲完，实战中更常见的是**按"哪类操作产生的倾斜"来选方案**。以下按场景整理速查手册：

### 7.5.1 join 产生的倾斜

| 手段 | 适用条件 | 要点 |
|---|---|---|
| **过滤异常值** | 空值、默认值 key 无业务意义 | join 前先 `WHERE key IS NOT NULL AND key != ''`，或把异常值**单独拆出来**走独立逻辑（比如统一写回一张异常表），不要混在主流程里陪跑 |
| **小表广播** | 一边是小表（<1~2GB） | `broadcast(df)` 或调 `spark.sql.autoBroadcastJoinThreshold`，map 端本地 join，零 shuffle |
| **加盐打散** | 两边都是大表 | 倾斜 key 加随机前缀 + 另一边膨胀对应倍数（即方案六/七） |
| **分桶表** | 两表都按 join key 分桶 | 预分桶使相同 key 的数据物理上就落在同一批文件，join 时直接 Bucket Join **跳过 shuffle**；要求两表桶数成倍数、分桶列 = join 列 |

```sql
-- 分桶表示例：建表时就按 user_id 分 64 个桶
CREATE TABLE user_action (user_id BIGINT, act STRING)
CLUSTERED BY (user_id) INTO 64 BUCKETS
STORED AS ORC;

-- 与同样按 user_id 分 64（或 128/32 等倍数）桶的 user_dim join 时，
-- Spark 可自动识别并触发 bucket join，免 shuffle
```

### 7.5.2 group by 产生的倾斜

| 手段 | 要点 |
|---|---|
| **双阶段聚合** | 即方案四：加盐局部聚合 → 去盐全局聚合。SQL 等价写法是 `GROUP BY concat(floor(rand()*10), '_', key)` 再二次聚合 |
| **map 端预聚合** | 用带 combiner 的算子（`reduceByKey` / `aggregateByKey`）代替 `groupByKey`；Spark SQL 会自动做 map 端部分聚合（`spark.sql.aggregate.partialAggregationEnabled` 相关优化），确认没被关闭 |
| **先过滤再聚合** | 聚合前先把不参与统计的行过滤掉（时间范围、状态值、空值），数据量减半，倾斜自然缓解——**最容易被忽略、性价比最高**的一招 |

> **map 端预聚合原理**：每个分区先把相同 key 局部聚合，shuffle 只传 `(key, 局部结果)`；不预聚合则 shuffle 传 `(key, 全部原始记录)`。数据量相差可达数量级。

### 7.5.3 count(distinct) 产生的倾斜

`count(DISTINCT key)` 会把**所有不同 key 的值拉到同一个 task** 做全局去重，key 一多必倾斜。三种替代：

**① 改写为 group by + count（精确，最常用）**：

```sql
-- 改写前：全部数据挤进一个 task 去重
SELECT count(DISTINCT user_id) FROM t;

-- 改写后：先分组（分组会分散到多个 task），再数行数，两阶段完成
SELECT count(*) FROM (
    SELECT user_id FROM t WHERE user_id IS NOT NULL GROUP BY user_id
) x;
```

**② HyperLogLog 近似去重（允许 1%~2% 误差时，性能最好）**：

```sql
-- 数据量极大、只要量级不要精确值时使用
SELECT approx_count_distinct(user_id) FROM t;
```

**③ 位图 Bitmap（distinct 值域有限时，精确且快）**：

```sql
-- 如 user_id 是 0~1000 万的整数 ID，可用 bitmap 精确去重（Spark 3.x + bitmasks UDF）
-- 每个值只占 1 bit，1 亿 ID 约 12MB，聚合开销极小
```

| 方案 | 精确性 | 性能 | 适用 |
|---|---|---|---|
| group by + count(*) | 精确 | 好 | 默认选择 |
| approx_count_distinct | ~1-2% 误差 | 极好 | 超大数据、看趋势 |
| Bitmap | 精确 | 极好 | distinct 值为有限整数集 |

### 7.5.4 空值、默认值导致的倾斜

脏 key（`null`、`''`、`0`、`-1`、`'unknown'`）是实践中最高频的倾斜元凶，因为所有"没有值"的记录都会聚到同一个 key 上：

```sql
-- 手段一：直接过滤（无业务意义时）
SELECT ... FROM t WHERE key IS NOT NULL AND key NOT IN ('', 'unknown', '-1');

-- 手段二：用随机数代替 null（空值有业务意义、不能丢时）
-- 把 null key 替换为随机数，让它们打散到不同 task，聚合后再单独归类
SELECT ... FROM (
    SELECT coalesce(key, concat('null_', floor(rand() * 100))) AS key, ...
    FROM t
) x;
```

> **选择原则**：空值对统计无意义 → 过滤；空值需要保留在结果里 → 随机数打散（或单独拆出来处理再 union 回去）。

### 7.5.5 动态分区写入倾斜

往 Hive 分区表写数据时，若数据集中在少数分区（如"日期+城市"里北上广深占大头），写这几个分区的 task 会远慢于其他，且**动态分区会在写入时缓存大量文件句柄**引发 OOM。

| 手段 | 做法 |
|---|---|
| **预创建分区** | 写入前把目标分区 `ALTER TABLE ... ADD PARTITION` 建好，减少动态推断分区的内存与句柄开销 |
| **关闭动态分区** | 业务允许时改为**静态分区**写入（`INSERT OVERWRITE TABLE t PARTITION(dt='2026-09-30')`），一次只写一个分区，最干净 |
| **开启倾斜优化** | Spark 3.x 的 AQE 自动倾斜处理（见下），对写入阶段的倾斜 task 自动拆分 |
| 分区裁剪 | 读取端配合 `WHERE dt=...` 谓词下推，别全表扫描再分区 |

```sql
-- 动态分区写入的典型配置（Spark 写 Hive）
SET spark.sql.sources.partitionOverwriteMode=dynamic;  -- 动态分区覆盖
SET spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict;  -- 允许全部动态分区
```

### 7.5.6 小文件问题与 AQE 自动优化

**小文件倾斜**是另一面的问题：分区过多过细 → 每个 task 只处理几百 KB → task 调度开销反而大于计算本身，NameNode 也被小文件拖垮。

**手段一：写入前合并小文件**。`repartition(n)` 控制输出文件数（n = 目标文件大小 / 每文件期望大小）：

```java
// 期望每文件 128MB，总数据 1GB → repartition(8)
df.repartition(8).write().parquet("output");
```

**手段二：AQE 自动合并小分区**。Spark 3.0+ 的自适应查询执行（Adaptive Query Execution，3.2 起默认开启）会在运行时根据**实际**统计信息自动做三件事：**合并小分区、拆分倾斜分区、切换 join 策略**：

```properties
# AQE 总开关（Spark 3.2+ 默认 true）
spark.sql.adaptive.enabled=true
# 自动合并过小的 shuffle 分区
spark.sql.adaptive.coalescePartitions.enabled=true
# 自动拆分倾斜分区（倾斜 join 的自动解药）
spark.sql.adaptive.skewJoin.enabled=true
# 判定倾斜的因子：分区数据量超过中位数 × 该因子即视为倾斜（默认 5）
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
# 倾斜分区的绝对阈值（默认 256MB）
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB
```

> **AQE 与手动方案的关系**：AQE 解决了大部分"通用倾斜"，让方案三/四/七的使用频率大幅下降；但 AQE 只作用于 shuffle read 阶段的自动拆分，**源头脏数据（空值）和大表间业务性热点 key 仍需手动方案**。生产实践 = AQE 打底 + 手动方案针对顽固倾斜。

---

## 7.6 Shuffle 调优参数

shuffle 调优参数解决的是"shuffle 已经不可避免时，让它少疼一点"：

| 参数 | 默认值 | 作用 | 调优建议 |
|---|---|---|---|
| `spark.shuffle.file.buffer` | 32k | shuffle write 的写盘缓冲大小 | 内存充足可调 64k，减少溢写次数，可提升 1%~5% |
| `spark.reducer.maxSizeInFlight` | 48m | shuffle read 每次拉取数据的缓冲大小 | 可调 96m，减少网络传输次数 |
| `spark.shuffle.io.maxRetries` | 3 | 拉取数据失败的重试次数 | 大作业（数十亿级）建议 60，避免 GC 停顿导致整个任务失败 |
| `spark.shuffle.io.retryWait` | 5s | 每次重试的等待间隔 | 配合上面调大（如 60s），提升稳定性 |
| `spark.shuffle.sort.bypassMergeThreshold` | 200 | map task 数低于该值且非聚合算子时启用 bypass（免排序） | 不需要排序时可调大，自动触发 bypass 机制 |
| `spark.sql.shuffle.partitions` | 200 | SQL/shuffle read 的分区数 | 数据量大时调大（1000~5000），配合 AQE 可让 AQE 自动向下合并 |
| `spark.shuffle.memoryFraction` | —（2.0 起废弃） | 旧版 shuffle 聚合内存占比 | 3.x 已改为统一内存管理，用 `spark.memory.fraction`（默认 0.6）替代 |
| `spark.shuffle.consolidateFiles` | —（2.0 起失效） | HashShuffleManager 的 consolidate 开关 | 3.x 只有 SortShuffleManager，此参数仅作历史了解 |

> **定位**：shuffle 参数调优在整个 Spark 性能优化中只占一小部分，大头永远是**代码写法（减少 shuffle）→ 资源配置 → 数据倾斜治理**。别舍本逐末。

---

## 7.7 高频面试题与调优口诀

### 面试题一：groupByKey vs reduceByKey 为什么性能差这么多？

**reduceByKey 有 map 端预聚合（combiner）**：每个分区先局部聚合，shuffle 只传 `(key, 局部和)`；**groupByKey 不预聚合**，shuffle 传全量原始记录。数据量一大，网络与磁盘开销相差数量级。

```
reduceByKey：分区内先 sum → shuffle 传 (key, 局部和) → 再 sum   [数据量小]
groupByKey ：分区内不聚合 → shuffle 传 (key, 全部 value) → 再 sum [数据量大]
```

### 面试题二：如何排查一场数据倾斜？

**看现象**（少数 task 极慢 / OOM）→ **Web UI 定位 stage**（task 数据量差 10 倍以上）→ **倒推代码**（数 shuffle 边界）→ **采样 key 分布**（sample + countByKey）→ **按场景选方案**（见 7.5）。

### 面试题三：Spark 3.x 的 AQE 如何改变数据倾斜治理？

运行时基于真实统计优化执行计划：合并小分区（治小文件）、拆分倾斜分区（治倾斜 join）、大表 join 自动转广播（治 join 策略失误）。把过去很多手动调优变成了默认行为，但源头脏数据与业务热点仍需手动治理。

### 调优口诀

1. **能不用 shuffle 就不用**：`map`/`filter`/`mapValues` 优于 `groupByKey`。
2. **能预聚合就预聚合**：`reduceByKey` / `aggregateByKey`，聚合前先过滤。
3. **小表 join 用广播**：彻底消除 shuffle；两表都大考虑分桶。
4. **脏 key 先清洗**：空值、默认值过滤或打散，是性价比最高的手段。
5. **聚合倾斜用加盐**：两阶段聚合是聚合类倾斜的标准解。
6. **去重别硬扛**：count distinct 改 group by + count，或上 HyperLogLog。
7. **AQE 打底**：3.x 先开 AQE，再对顽固倾斜手动治理。
8. **观察 Spark UI**：一切调优从"看见倾斜"开始。

---

## 7.8 本章小结

1. **Shuffle** = 同 key 数据跨节点重分配，代价 = 磁盘 IO + 序列化 + 网络；宽依赖是判定标准。
2. **ShuffleManager 演进**的核心是减少文件数量：Hash（5000 文件）→ consolidate（1000）→ Sort（50）；bypass 机制免排序。
3. **数据倾斜**只发生在 shuffle：同 key 必落同一 task，热点 key 拖垮全局；定位靠 Web UI 找 stage + 倒推代码 + 采样 key。
4. **八大通用方案**：源头预处理 → 过滤 key → 提并行度 → 两阶段聚合 → 小表广播 → 分拆 join → 扩容 join → 组合拳。
5. **场景速查**：join 倾斜（过滤异常值/广播/加盐/分桶）、group by 倾斜（双阶段/map 端预聚合/先过滤）、count distinct（group by 改写/HLL/位图）、空值倾斜（过滤/随机数）、动态分区（预建/静态写入）、小文件（repartition/AQE 合并）。
6. **Spark 3.x 心法**：AQE 自动兜底 + 手动方案治顽固倾斜 + shuffle 参数锦上添花。
