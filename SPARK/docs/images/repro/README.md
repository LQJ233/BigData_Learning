# Web UI 截图复现说明

`docs/15-SparkWebUI.md` 里的截图，全部来自真实运行的 **Spark 3.3.1**
standalone 集群（1 Driver + 3 Executor），不是示意图、也不是手绘 mockup。

## 环境

```text
Spark   3.3.1（官方 bin 发行版 spark-3.3.1-bin-hadoop3）
JDK     17
模式    standalone（1 Master + 1 Worker，Worker 上起 3 个 Executor）
Web UI  http://localhost:4040
```

## 启动集群与提交作业

```bash
# 1. 启动 standalone 集群
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

# 2. 提交演示程序（注意用 daemon / 后台会话跑，进程退出 UI 就没了）
$SPARK_HOME/bin/spark-submit \
  --master spark://localhost:7077 \
  --name "Spark UI Demo" \
  --class demo.SparkUiDemo \
  --executor-cores 1 --total-executor-cores 4 \
  --executor-memory 1g --driver-memory 3g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=true \
  SparkUiDemo.java 编译后的 jar
```

## 演示程序制造了哪些现场

| 方法 | 制造的现象 | 对应截图 |
|---|---|---|
| `rddShuffleAndCache` | RDD shuffle（reduceByKey / groupByKey / sortByKey）+ `persist` | `webui-jobs`、`webui-stages`、`webui-storage` |
| `skewedAggregation` | 90% 数据落到同一个热点 Key，用 `groupByKey` 放大倾斜 | `webui-stage-skew` |
| `dataFrameJoin` | DataFrame join（广播 join） | `webui-job-detail`、`webui-sql-detail` |
| `sqlAggregation` | SQL 聚合 + 窗口函数 | `webui-sql` |
| `streaming` | Structured Streaming 微批（60 秒一批） | `webui-streaming` |

## 截图与标注约定

```text
viewport  1920 宽，长页面加高（最多 1750），保证整页可见
标注      只有红色方框 + 箭头 + 中文说明，不使用任何填充色
标号      只写数字（红字 + 白描边），不用实心圆
```

## 三个容易踩的坑

```text
① 本地模式（local[*]）下 Executors 页签只有一行 driver ——
   要用 standalone / YARN 才能看到多个 Executor。

② Structured Streaming 页签的 URL 是 /StreamingQuery/（不是 /streaming/），
   而且必须有「活跃」的流查询，查询 stopped 之后页签就消失了。

③ take() 只会跑前几个分区，看不到完整的分区级倾斜分布；
   要观察倾斜请用 collect() / count() 之类的全量行动算子。
```

`spark_diag.py` 是用来生成 `docs/images/spark-*.svg` 那些示意图的脚本
（架构图 / 流程图 / 原理图），与截图用的是同一套配色。
