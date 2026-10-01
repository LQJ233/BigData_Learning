# BigData 学习项目 · Spark 学习文档

> 本文档由项目内已有的 Spark 代码整理而成，保留原始代码与注释笔记，并补充了知识点说明。
> 对应源码模块：`Spark/`（Spark 3.3.1 / Scala 2.12 / Java API / Java 8）

## 文档目录

| 文档 | 内容 | 对应源码包 |
|---|---|---|
| [01-Spark基础与开发环境](./01-Spark基础与开发环境.md) | Spark 基本格式（新旧 API 对比）、Driver/Executor 架构、依赖版本、作业提交 | `com.Sparkcore`（Spark00） |
| [02-Spark数据导入与数据结构](./02-Spark数据导入与数据结构.md) | RDD / DataFrame / Dataset 区别，从集合/文本/JSON/CSV/Parquet/MySQL 导入，Schema 与分区原理 | `com.Sparkcore`（Spark01）、`org.SparkSQL`（导入json）、`SparkStream`（test1） |
| [03-RDD版转换算子](./03-RDD版转换算子.md) | RDD 转换算子：map / flatMap / groupBy / filter / distinct / sort / mapValues / groupByKey / reduceByKey / sortByKey / mapPartitions / join / coalesce / aggregateByKey 等 | `com.Sparkcore`（Spark02xx） |
| [04-RDD版行动算子](./04-RDD版行动算子.md) | RDD 行动算子：count / collect / first / take / countByKey / reduce / saveAsTextFile / foreach / foreachPartition 等 | `com.Sparkcore`（Spark03xx） |
| [05-DataFrame版转换算子](./05-DataFrame版转换算子.md) | DataFrame 转换算子：select / withColumn / filter / groupBy+agg / join / explode 等（列式 API） | `org.SparkSQL` |
| [06-DataFrame版行动算子](./06-DataFrame版行动算子.md) | DataFrame 行动算子：show / count / collect / describe / printSchema / write 等 | `org.SparkSQL` |
| [07-Shuffle专题](./07-Shuffle专题.md) | Shuffle 机制、宽窄依赖、ShuffleManager 演进（Hash/consolidate/Sort/bypass）、shuffle 调优参数 | — |
| [08-数据倾斜专题](./08-数据倾斜专题.md) | 现象与定位三步法、八大通用方案、join/group by/count distinct/空值/动态分区/小文件六大场景速查、AQE | — |
| [09-SparkSQL基础](./09-SparkSQL基础.md) | 导入 JSON 建临时视图、UDF / UDAF / UDTF 三种自定义函数 | `org.SparkSQL` |
| [10-SQL语法与内置函数](./10-SQL语法与内置函数.md) | SELECT 全家桶、7 种 JOIN、子查询、临时视图 vs 全局临时视图、内置函数速查表 | — |
| [11-窗口函数与高阶函数](./11-窗口函数与高阶函数.md) | row_number/rank/dense_rank、lead/lag、聚合开窗、TopN 完整案例、transform/filter 等高阶函数 | — |
| [12-SparkSQL数据工程与Hive](./12-SparkSQL数据工程与Hive.md) | write 4 种 mode、partitionBy/bucketBy 分区分桶写出、saveAsTable 与 Catalog、Hive 集成 | — |
| [13-SparkSQL原理与性能](./13-SparkSQL原理与性能.md) | Catalyst 优化流程、谓词下推与列裁剪、explain 执行计划、AQE | — |
| [14-SparkStreaming](./14-SparkStreaming.md) | Spark Streaming + JDBC 读取 MySQL 示例 | `SparkStream` |
| [15-SparkWebUI](./15-SparkWebUI.md) | Spark Web UI 运维界面详解（Jobs/Stages/SQL/Executors/Storage/Environment） | — |

## 学习路径建议

1. **先读 01**：搞懂 `SparkSession` 与 `JavaSparkContext` 两种 API 的关系，以及 Driver/Executor 架构（这是理解后续一切的基础）。
2. **再读 02**：搞懂 RDD / DataFrame / Dataset 三者的区别，以及如何从不同数据源导入数据；同时理解 RDD 的分区机制。
3. **读 03、04**：RDD 版转换/行动算子，重点理解哪些算子产生 **shuffle**、`collect` 的内存风险、`foreach` vs `foreachPartition`。
4. **读 05、06**：DataFrame 版转换/行动算子，掌握列式 API（`select`/`withColumn`/`groupBy+agg`）与调试三件套（`show`/`printSchema`/`describe`）。
5. **读 07、08**：先 Shuffle 机制（为什么慢），再数据倾斜（怎么治）——性能优化和面试的核心专题。
6. **读 09~13**：SparkSQL 全家桶，按顺序走：自定义函数（09）→ SQL 语法与 JOIN（10）→ 窗口函数（11，TopN/环比/连续登录必考）→ 数据写出与 Hive（12）→ Catalyst 原理与 explain（13）。
7. **读 14**：了解老的 DStream API（`JavaStreamingContext`），生产上 Spark 3.x 更推荐 Structured Streaming。
8. **最后读 15**：学会用 Spark Web UI（Jobs/Stages/SQL/Executors 等页面）监控运行、排查性能问题，与 08/13 章形成"调优工具链闭环"。

## Spark 源码文件状态总览

| 文件 | 状态 | 说明 |
|---|---|---|
| [Spark00_Spark基本格式.java](../src/main/java/com/Sparkcore/Spark00_Spark基本格式.java) | ✅ 完整 | 含新旧 API 对比注释，CSV 路径已统一为项目内路径 |
| [Spark01_数据导入.java](../src/main/java/com/Sparkcore/Spark01_数据导入.java) | ✅ 完整 | parallelize / textFile / saveAsTextFile，分区原理注释详细 |
| [Spark0201_转换算子_Map映射.java](../src/main/java/com/Sparkcore/Spark0201_转换算子_Map映射.java) | ⚠️ 含伪代码 | lambda 写法笔记有价值，但有未完成语句，当前不能编译 |
| [Spark0202_转换算子_flapMap.java](../src/main/java/com/Sparkcore/Spark0202_转换算子_flapMap.java) | ⚠️ 仅注释 | 类体为空，知识点在注释里 |
| [Spark0203_转换算子_groupBy分组.java](../src/main/java/com/Sparkcore/Spark0203_转换算子_groupBy分组.java) | ✅ 完整 | 两种分组键类型示例 |
| [Spark0204_转换算子_filter过滤.java](../src/main/java/com/Sparkcore/Spark0204_转换算子_filter过滤.java) | ✅ 完整 | 可直接运行 |
| [Spark0205_转换算子_distinct去重.java](../src/main/java/com/Sparkcore/Spark0205_转换算子_distinct去重.java) | ✅ 完整 | 含去重原理注释 |
| [Spark0206_转换算子_sort排序.java](../src/main/java/com/Sparkcore/Spark0206_转换算子_sort排序.java) | ⚠️ 代码被注释 | 知识点完整，可参考注释恢复代码 |
| [Spark0207_转换算子_mapVaules.java](../src/main/java/com/Sparkcore/Spark0207_转换算子_mapVaules.java) | ✅ 已补全 | 已补全为可运行示例，含知识点注释 |
| [Spark0208_转换算子_groupByKey.java](../src/main/java/com/Sparkcore/Spark0208_转换算子_groupByKey.java) | ✅ 已补全 | 已补全为可运行示例，含知识点注释 |
| [Spark0209_转换算子_reduceByKey.java](../src/main/java/com/Sparkcore/Spark0209_转换算子_reduceByKey.java) | ✅ 已补全 | 已补全为可运行示例，含知识点注释 |
| [Spark0210_转换算子_sortByKey.java](../src/main/java/com/Sparkcore/Spark0210_转换算子_sortByKey.java) | ✅ 已补全 | 已补全为可运行示例，含知识点注释 |
| [Spark0301_行动算子_count.java](../src/main/java/com/Sparkcore/Spark0301_行动算子_count.java) | ✅ 已补全 | 已补全为可运行示例，保留原注释 |
| [Spark0302_行动算子_collect.java](../src/main/java/com/Sparkcore/Spark0302_行动算子_collect.java) | ✅ 已补全 | 已补全为可运行示例，保留原注释 |
| [Spark0303_行动算子_first.java](../src/main/java/com/Sparkcore/Spark0303_行动算子_first.java) | ✅ 已补全 | 已补全为可运行示例，保留原注释 |
| [Spark0304_行动算子_take.java](../src/main/java/com/Sparkcore/Spark0304_行动算子_take.java) | ✅ 已补全 | 已补全为可运行示例，保留原注释 |
| [Spark0305_行动算子_countByKey.java](../src/main/java/com/Sparkcore/Spark0305_行动算子_countByKey.java) | ✅ 已补全 | 已补全为可运行示例，保留原注释 |
| [Spark0306_行动算子_saveAsTextFile.java](../src/main/java/com/Sparkcore/Spark0306_行动算子_saveAsTextFile.java) | ✅ 已补全 | 已补全为可运行示例，保留原注释 |
| [Spark0307_行动算子_saveAsObjectFile.java](../src/main/java/com/Sparkcore/Spark0307_行动算子_saveAsObjectFile.java) | ✅ 已补全 | 已补全为可运行示例，保留原注释 |
| [Spark0308_行动算子_foreach.java](../src/main/java/com/Sparkcore/Spark0308_行动算子_foreach.java) | ✅ 已补全 | 已补全为可运行示例，保留原注释 |
| [Spark0309_行动算子_foreachPartition.java](../src/main/java/com/Sparkcore/Spark0309_行动算子_foreachPartition.java) | ⚠️ 空类 | 知识点在注释中 |
| [Spark0310_行动算子_foreachPartition.java](../src/main/java/com/Sparkcore/Spark0310_行动算子_foreachPartition.java) | ⚠️ 代码被注释 | 知识点在注释中 |
| [导入json.java](../src/main/java/org/SparkSQL/导入json.java) | ✅ 完整 | 可运行（路径已统一为项目内相对路径） |
| [UDF.java](../src/main/java/org/SparkSQL/UDF.java) | ✅ 完整 | 可运行（路径已统一为项目内相对路径） |
| [UDAF.java](../src/main/java/org/SparkSQL/UDAF.java) | ✅ 完整 | 可运行（路径已统一为项目内相对路径） |
| [test1.java](../src/main/java/SparkStream/test1.java) | ✅ 完整 | 流式读 MySQL，密码已改为环境变量读取 |

> **说明**：已补全的算子文件，其代码即为可运行的源码示例（含知识点注释）。

## 已知问题与注意事项

1. ~~重复模块~~：✅ 已清理。根 `pom.xml` 仅保留 `Spark` 与 `Flink` 两个模块（此前 `SPARK` 与 `Spark` 在 macOS 大小写不敏感文件系统上是同一目录的两种写法）。
2. ~~Windows 遗留路径~~：✅ 已统一为项目内相对路径（如 `Spark/src/main/data/user.json`）。
3. **编译不过的文件**：`Spark0201`（伪代码）、`Flink01_从mysql读取数据`（依赖被注释）等，详见各文档标注。
4. ~~硬编码数据库密码~~：✅ 已改为环境变量 `MYSQL_PASSWORD` 读取，本地运行时需先设置该环境变量。
5. ~~git 仓库尚未提交~~：✅ 已推送到 GitHub（`main` 分支）。

## 数据文件（Spark/src/main/data）

| 文件 | 内容 | 行数 |
|---|---|---|
| user.json | 用户姓名/年龄（UDF、UDAF 示例数据） | 8 |
| iris.json | 鸢尾花数据集（导入 JSON 备用，已从公开源重建） | 150 |
| user_visit_action | 用户访问行为数据 | 18 万 |
| city_info | 城市信息 | 26 |
| product_inf0.txt | 商品信息 | 100 |
| Tariff Calculations.csv / plus Population.csv | 关税计算示例数据 | ⚠️ 已丢失，待用户提供备份 |
