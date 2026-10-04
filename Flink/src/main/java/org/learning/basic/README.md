# org.learning.basic —— 可运行的 Flink DataStream 模板

对应文档：`docs/01-基础使用.md` 第 1 节。

模板按「**主流程只负责串联，业务逻辑拆成独立 Function**」组织，
每个 Function 都能单独 new 出来做单元测试，出问题时也能直接把断点打在那个类里。

## 文件清单

| 文件 | 角色 | 说明 |
|---|---|---|
| `Order.java` | POJO | 业务对象：userId / amount / count |
| `OrderAmountJob.java` | **生产模板** | Kafka → 过滤 → 解析 → ×2 → 分组累加 → Kafka |
| `OrderAmountLocalDemo.java` | **本地 Demo** | 同样的算子，Source/Sink 换成内存数据 + print，不用装 Kafka 就能跑 |
| `OrderProcessDemo.java` | ProcessFunction Demo | 用 `keyBy().process()` 演示状态与定时器 |
| `function/ValidRecordFilter.java` | FilterFunction | 过滤格式不合法的记录 |
| `function/OrderParseFunction.java` | **RichMapFunction** | 解析 + if 判断，并注册两个计数器 |
| `function/AmountDoubleFunction.java` | MapFunction | 金额 ×N |
| `function/UserAmountSumFunction.java` | **ReduceFunction** | 分组累加（等价于 GROUP BY 的 SUM / COUNT） |
| `function/UserAmountProcessFunction.java` | **KeyedProcessFunction** | 状态 + 定时器 + 上下文 |
| `function/SlowOrderSource.java` | RichSourceFunction | Demo 专用：每 3 秒发一条，让定时器有机会触发 |

## 怎么跑

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
cd Flink
mvn -q compile

# 1) 本地 Demo：不需要 Kafka，打印分组累加结果
mvn -q exec:java -Dexec.mainClass=org.learning.basic.OrderAmountLocalDemo

# 2) ProcessFunction Demo：约 15 秒，能看到状态累加与定时器触发
mvn -q exec:java -Dexec.mainClass=org.learning.basic.OrderProcessDemo

# 3) 生产模板：需要可用的 Kafka，改掉 OrderAmountJob 里的 BOOTSTRAP_SERVERS
mvn -q exec:java -Dexec.mainClass=org.learning.basic.OrderAmountJob
```

## 两个已知噪音（都不是错误）

本地跑的时候控制台可能冒出下面两行，都不影响结果：

| 输出 | 原因 | 处理 |
|---|---|---|
| `The module flink-runtime-web could not be found in the class path` | 本地执行时 Flink 想启动 Web UI，但工程里没引那个 jar | 忽略；想看 Web UI 就提交到集群，见 `docs/08-生产运维` |
| `Error during shutdown of MemoryExecutionGraphInfoStore via JVM shutdown hook` | `exec-maven-plugin` 先关掉了类加载器，JVM 关闭钩子再去清理时找不到类 | 忽略，作业此时已经跑完 |

## 和文档的对应关系

```text
第 1 步 创建执行环境   → getExecutionEnvironment() + env.setParallelism(4)
第 2 步 创建 Source    → KafkaSource.builder() + env.fromSource(...)
                         （本地 Demo 用 fromElements / SlowOrderSource）
第 3 步 Transformation → ValidRecordFilter → OrderParseFunction
                         → AmountDoubleFunction → keyBy().reduce()
第 4 步 创建 Sink      → KafkaSink.builder() + sinkTo(...)（Demo 用 print()）
第 5 步 execute        → env.execute("...")
```
