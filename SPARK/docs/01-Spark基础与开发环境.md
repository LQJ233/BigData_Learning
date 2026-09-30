# 01 · Spark 基础与开发环境

> 对应源码：[Spark00_Spark基本格式.java](../src/main/java/com/Sparkcore/Spark00_Spark基本格式.java)
>
> 数据导入、RDD 分区机制，以及 RDD / DataFrame / Dataset 三者的区别已独立成篇，见 [02-Spark数据导入与数据结构](./02-Spark数据导入与数据结构.md)。

## 1.1 两种 API 写法：JavaSparkContext vs SparkSession

Spark 开发有两套 API，代码注释里做了详细对比：

| 对比项 | 老写法：`SparkConf` + `JavaSparkContext` | 新写法：`SparkSession` |
|---|---|---|
| 入口对象 | `JavaSparkContext`（RDD 编程入口） | `SparkSession`（统一入口） |
| 能力范围 | 仅 RDD | DataFrame / Dataset / SparkSQL / SparkStreaming 全支持 |
| 出现背景 | Spark 1.x 时代 | Spark 2.x 之后推荐 |
| 配置方式 | `new SparkConf().setMaster(...).setAppName(...)` | `SparkSession.builder().config(...).appName(...).master(...)` |

**关键配置项说明（来自源码注释）：**

- `setMaster("local[*]")`：指定运行环境。`local` 表示本地单机运行（非集群），`[*]` 表示使用本机所有 CPU 核心。
- `setAppName("sparkCore")`：应用名称，会显示在 Spark UI 中，便于监控识别。
- `config("spark.driver.host", "localhost")`：**不加这句会报错**——本地运行时 Driver 的 IP 可能变动，显式指定为 localhost 可以避免。

### 示例：SparkSession 读取 CSV

> 对应源码：[Spark00_Spark基本格式.java](../src/main/java/com/Sparkcore/Spark00_Spark基本格式.java)

```java
package com.Sparkcore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Spark00_Spark基本格式
{
    public static void main(String[] args)
    {
        // 老写法（注释部分，仅支持 RDD，不支持 DataFrame/Dataset/SparkSQL/SparkStreaming）：
        // SparkConf conf = new SparkConf()
        //         .setMaster("local[*]")
        //         .setAppName("sparkCore")
        //         .set("spark.driver.host","localhost");//不加这句会报错
        // JavaSparkContext sc = new JavaSparkContext(conf);

        // 新写法：创建或获取一个 SparkSession 对象
        SparkSession spark = SparkSession
                .builder()
                .config("spark.driver.host","localhost")//不加这句会因为ip变动而报错
                .appName("My Spark Application")
                .master("local[*]")  // 使用本地所有可用的核心进行计算
                .getOrCreate();

        // 导入数据为 Dataset，每行封装为一个 Row 对象
        Dataset<Row> df = spark
                .read()
                .format("csv")            // 指定读取的是 csv 数据
                .option("header", "true") // 假设数据文件有标题行
                .option("sep", ";") // 该数据文件为分号分隔（Tariff Calculations.csv）
                .load("Spark/src/main/data/Tariff Calculations.csv");

        // 显示数据的前几行
        df.show();

        spark.stop();
    }
}
```


---

## 1.2 运行架构：Driver / Executor 与本地模式



| 角色 | 谁在跑 | 职责 |
|---|---|---|
| **Driver** | 你写的 `main` 方法所在的进程 | 解析程序、构建 DAG（执行计划）、向集群申请资源、调度 Task、回收结果 |
| **Executor** | 集群里的工作节点进程 | 真正执行 Task 计算、缓存数据（`cache`/`persist`）、把结果返回给 Driver |

```
                    ┌─────────────────────────┐
  你的 main 方法  ──▶│  Driver（调度 + 收集结果）  │
                    └───────────┬─────────────┘
                                │ 分发 Task（任务）
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
   │  Executor 1 │     │  Executor 2 │     │  Executor 3 │   ← 真正干活
   │  (执行 Task) │     │  (执行 Task) │     │  (执行 Task) │
   └─────────────┘     └─────────────┘     └─────────────┘
```

**关键点**：`SparkSession` / `JavaSparkContext` 是在 **Driver** 里创建的；RDD 的转换算子在 Driver 端构建执行计划，真正的计算在 **Executor** 端执行。

### 本地模式（local）的本质

`setMaster("local[*]")` 里的 `local` 表示**本地模式**——Driver 和 Executor **跑在同一个 JVM 里**，用多线程模拟分布式，不是真正的多机集群：

| 写法 | 含义 |
|---|---|
| `local` | 1 个线程（单核），无法并行 |
| `local[2]` | 2 个线程 |
| `local[*]` | 本机所有 CPU 核心 |
| `local[2, 3]` | 2 个"伪 Executor 进程"，每个 3 个线程（更接近真实集群） |

> **为什么要理解这一点**：本地模式下 Driver 和 Executor 共享一个进程，所以：
> - `collect()` 把数据拉回 Driver 时，本地模式数据本来就在同一进程里，看不出内存风险；但**在真实集群上，数据会通过网络跨节点传到 Driver**，数据量大就 OOM——这是 04 章 `collect` 面试题的本质。
> - 本地模式适合学习、调试；生产环境用 `yarn` / `standalone` / `k8s` 等集群模式，`setMaster` 参数会相应改变。

---

## 1.3 依赖坐标与版本对应关系

Spark 的 Maven 依赖有个**新手极容易搞混的点**：`artifactId` 里 `_2.12` 是 **Scala 版本**，不是 Spark 版本。以本项目 `Spark/pom.xml` 为例：

```xml
<!-- 完整坐标：groupId + artifactId + version -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.12</artifactId>   <!-- _2.12 = Scala 2.12 -->
    <version>3.3.1</version>                    <!-- 3.3.1 = Spark 版本 -->
</dependency>
```

| 坐标组成 | 含义 | 本项目取值 |
|---|---|---|
| `groupId` | 组织标识 | `org.apache.spark` |
| `artifactId` 前半段 | 模块名 | `spark-core` / `spark-sql` 等 |
| `artifactId` 的 `_2.12` | **Scala 版本** | `_2.12`（Scala 2.12） |
| `version` | **Spark 版本** | `3.3.1` |

### 三个版本必须匹配（否则运行时报错）

Spark 3.x 运行对 **Spark 版本 ↔ Scala 版本 ↔ Java 版本** 有严格的对应关系：

| Spark 版本 | 配套 Scala | 配套 Java |
|---|---|---|
| 3.3.1（本项目） | 2.12（或 2.13） | 8 / 11 / 17 |
| 3.2.x | 2.12 / 2.13 | 8 / 11 |
| 3.0.x | 2.12 | 8 / 11 |
| 2.4.x | 2.11 / 2.12 | 8 |

> **常见坑**：
> - 把 `spark-core_2.12` 写成 `spark-core_2.11`（Scala 版本搞错）→ 依赖能下但运行时报 `NoSuchMethodError` 等类加载错误。
> - 多个 Spark 模块（core/sql/streaming）的 **version 必须完全一致**，否则会出现类冲突。

### 本项目 Spark 模块的完整依赖

| 依赖 | 用途 |
|---|---|
| `spark-core_2.12` 3.3.1 | RDD 编程核心 |
| `spark-sql_2.12` 3.3.1 | DataFrame / Dataset / SparkSQL |
| `spark-mllib_2.12` 3.3.1 | 机器学习 |
| `spark-streaming_2.12` 3.3.1 | 流处理（DStream） |
| `spark-streaming-kafka-0-10_2.12` 3.3.1 | Kafka 集成 |
| `mysql-connector-java` 8.0.28 | JDBC 读 MySQL 的驱动 |

> **要点**：`spark-sql` 依赖 `spark-core`，所以导入 sql 模块后 RDD 相关类也能用；日常开发**只需按需引入**用到的模块即可，不必全引。

---

## 1.4 作业提交：如何把代码跑起来

前面章节都在写代码、本地 `main` 里跑。但真正的 Spark 作业要打包成 **jar** 后，用 **`spark-submit`** 命令提交到集群执行。这一节讲清"写好代码之后怎么办"。

### ① 从"写代码"到"提交"的完整流程

```
1. 写代码（Java/Scala，含 main 方法）
        ↓
2. 打包：mvn package  →  target/Spark-1.0-SNAPSHOT.jar
        ↓
3. 提交：spark-submit --class 主类 --master ... jar包
        ↓
4. 集群运行，日志/结果回 Driver，监控看 Web UI（见 10 章）
```

### ② 打包（以本项目为例）

本项目是 Maven 工程，在根目录执行：

```bash
# 打包（会先编译、跑测试，产物在 target/ 下）
mvn clean package

# 跳过测试打包（更快）
mvn clean package -DskipTests
```

打包后得到 `Spark/target/Spark-1.0-SNAPSHOT.jar`，这就是要提交的作业包。

> ⚠️ **注意**：Spark 相关的依赖（`spark-core`、`spark-sql` 等）在集群上已由 Spark 环境提供，打包时通常设为 `provided` scope，避免把几百 MB 的 Spark 打进 jar。本项目 pom 未设 scope（学习场景本地跑无所谓），生产上记得改。

### ③ spark-submit 命令与核心参数

```bash
spark-submit \
  --class com.Sparkcore.Spark00_Spark基本格式 \   # 主类（含 main 方法）
  --master local[*] \                              # 运行模式
  --deploy-mode client \                           # Driver 部署位置
  --driver-memory 1g \                             # Driver 内存
  --executor-memory 1g \                           # 每个 Executor 内存
  --executor-cores 2 \                             # 每个 Executor 核数
  --num-executors 2 \                              # Executor 数量
  Spark/target/Spark-1.0-SNAPSHOT.jar              # 作业包
```

**核心参数速查表：**

| 参数 | 作用 | 常用取值 |
|---|---|---|
| `--class` | 指定主类（含 main 的类） | 全限定类名 |
| `--master` | 运行模式 | `local[*]` / `yarn` / `spark://host:7077` |
| `--deploy-mode` | Driver 在哪运行 | `client`（Driver 在提交机）/ `cluster`（Driver 在集群） |
| `--driver-memory` | Driver 内存 | `1g` / `2g` |
| `--executor-memory` | 每个 Executor 内存 | `1g` / `4g` |
| `--executor-cores` | 每个 Executor 核数 | `2` / `4` |
| `--num-executors` | Executor 数量 | `2` / `10` |
| `--conf` | 动态传 Spark 配置 | `--conf spark.sql.shuffle.partitions=200` |
| `--name` | 应用名称（显示在 Web UI） | 自定义 |
| `--jars` | 附加依赖 jar（如 MySQL 驱动） | 逗号分隔的 jar 路径 |

### ④ client 模式 vs cluster 模式

| 模式 | Driver 位置 | 适用场景 |
|---|---|---|
| `client` | 提交命令的机器（你的电脑/跳板机） | 交互式调试、本地提交，日志直接打印到终端 |
| `cluster` | 集群中的某个节点 | 生产环境、提交后客户端可断开，日志看 Web UI |

> **区别**：`client` 模式 Driver 在你的提交机，断了 SSH 作业可能挂；`cluster` 模式 Driver 在集群里，提交后即可关闭终端，作业继续跑。

### ⑤ 本地模式 vs 集群模式提交

```bash
# 本地模式（学习、调试）：Driver 和 Executor 都在本机
spark-submit --master local[*] --class com.Sparkcore.Spark00_Spark基本格式 Spark/target/Spark-1.0-SNAPSHOT.jar

# YARN 集群模式（生产）：资源交给 YARN 调度
spark-submit --master yarn --deploy-mode cluster \
  --num-executors 10 --executor-memory 4g --executor-cores 2 \
  --class com.Sparkcore.Spark00_Spark基本格式 Spark/target/Spark-1.0-SNAPSHOT.jar
```

### ⑥ SparkSQL 作业怎么提交

SparkSQL 作业本质也是 Spark 应用，提交方式有三种：

**方式 1：直接命令行交互（spark-sql，适合临时查询）**

```bash
spark-sql --master local[*] -e "select * from user where age > 18"
```

**方式 2：spark-submit 提交 SQL 文件（适合固定脚本）**

```bash
spark-sql --master local[*] -f query.sql
```

**方式 3：写在 Java 代码里，用 spark-submit 提交（本项目做法）**

```java
// 代码里 spark.sql("...")，然后照常 spark-submit 提交
spark.sql("select name, avg(age) from user group by name").show();
```

```bash
spark-submit --class org.SparkSQL.UDAF --master local[*] Spark/target/Spark-1.0-SNAPSHOT.jar
```

> **总结**：RDD 作业和 SparkSQL 作业提交方式完全一样，都是 `spark-submit --class ... jar`。区别只在代码里用的是 `spark.sql()` 还是 RDD 算子。

### ⑦ 提交时常见的动态配置（--conf）

除了命令行参数，还可以用 `--conf` 动态传配置，覆盖代码里的设置：

```bash
spark-submit --conf spark.sql.shuffle.partitions=500 \
             --conf spark.executor.memory=2g \
             --class ... jar
```

| 常用 conf | 作用 |
|---|---|
| `spark.sql.shuffle.partitions` | shuffle 分区数（默认 200，数据大要调大） |
| `spark.executor.memory` | Executor 内存 |
| `spark.default.parallelism` | RDD 默认并行度 |
| `spark.sql.adaptive.enabled` | 自适应执行（Spark 3 默认开） |
