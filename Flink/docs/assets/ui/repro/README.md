# 07 / 08 截图复现工程

`docs/` 下各章引用的 `assets/ui/*.png` 标注图，都是在本机真实运行的 Flink 集群上
截图得到的，不是官方文档截图，也不是手绘示意图。
架构图 / 流程图类示意图则在 `docs/assets/diagrams/*.svg`，由脚本生成。

## 环境

```text
Flink   2.3.0（bin 发行版，bin/start-cluster.sh 本地会话集群）
JDK     17
Maven   3.9+
Web UI  http://localhost:8081
```

`conf/config.yaml` 关键改动：

```yaml
taskmanager:
  numberOfTaskSlots: 8       # 默认 1，截图需要多并行度

restart-strategy:
  type: fixed-delay          # 让失败作业反复重启，便于截"重启循环"
  fixed-delay:
    attempts: 100
    delay: 5 s

# 08-08 的 FlameGraph / Profiler 截图需要这两个开关（默认关闭）
rest:
  flamegraph:
    enabled: true
  profiling:
    enabled: true
```

## 编译与提交

```bash
# 1. 编译（flink 依赖 scope=provided，集群 lib 里已有）
mvn -q package

# 2. 提交（在 Flink 发行版根目录）
./bin/flink run -d -c demo.BackpressureJob target/flinkui-demo.jar
./bin/flink run -d -c demo.SkewJob         target/flinkui-demo.jar
./bin/flink run -d -c demo.WatermarkJob    target/flinkui-demo.jar
./bin/flink run -d -c demo.FailingJob      target/flinkui-demo.jar
./bin/flink run -d -c demo.CheckpointFailJob target/flinkui-demo.jar
./bin/flink run -d -c demo.StateJob        target/flinkui-demo.jar
./bin/flink run -d -c demo.SqlWindowJob    target/flinkui-demo.jar
./bin/flink run -d -c demo.SqlJoinJob      target/flinkui-demo.jar

# 从 Savepoint 恢复并把并行度改成 6（09 / 08-03 用到）
./bin/flink savepoint <job-id> file:///tmp/savepoints -m localhost:8081
./bin/flink run -d -s file:///tmp/savepoints/savepoint-xxxx \
  -c demo.StateJob target/flinkui-demo.jar 6
```

## 每个 Job 对应哪张图

| 类 | 制造的现场 | 对应截图 |
|---|---|---|
| `BackpressureJob` | 64 个均匀 Key + 每条 sleep 15ms 的慢算子 → 全 Subtask 反压 | `07-03-*`、`07-01-jobgraph.png`、`07-04-ckpt-slow-*` |
| `SkewJob` | 90% 记录落到同一个热点 Key → 单个 Subtask 倾斜 | `07-02-*`、`07-03-backpressure-skew.png` |
| `StateJob` | 限速 Source + KeyedState → 健康 Checkpoint / Savepoint | `07-04-ckpt-ok-*`、`08-03-*` |
| `WatermarkJob` | 快慢两路 Source union → Watermark 被慢路拖住 | `08-06-watermarks.png` |
| `FailingJob` | 处理到第 3000 条抛异常 + fixed-delay 重启 | `08-02-*`、`08-07-exceptions.png` |
| `CheckpointFailJob` | 强反压 + 1 秒 Checkpoint 超时 → Failed 计数持续上涨 | 用于验证超时类失败 |
| `SqlWindowJob` | Table API + Window TVF → SQL 的 JobGraph | `05-01`、`05-02`、`06-01` |
| `SqlJoinJob` | Table API + Interval Join → 双输入 JobGraph | `05-03`、`05-04` |

## Web UI 全覆盖截图

`08-08-WebUI操作与排查手册.md` 覆盖了 Web UI 的每个菜单与功能块，
对应截图以 `webui-` 开头，包括：

```text
webui-applications / webui-application-detail      Applications 与详情页
webui-submit-form                                  Submit New Job 的完整表单
webui-job-configuration / webui-job-rescales       Job 级 Configuration / Rescales
webui-vertex-detail / -taskmanagers / -accumulators / -flamegraph   算子抽屉
webui-tm-more-menu                                 算子 TaskManagers 的 More 菜单
webui-tm-profiler / webui-tm-stdout / webui-tm-logdetail            TaskManager
webui-jm-metrics / -configuration / -logs / -loglist / -profiler    JobManager
```

复现这些页面时要注意两点：

```text
① JobManager 的 Configuration / Log List 直接改 URL 会回落到 Metrics，
   需要在页面里点页签（截图脚本里用的是 click 而不是直接导航）。
② 算子抽屉的 FlameGraph、进程的 Profiler 默认关闭，必须先打开上面的 rest.* 开关。
```

## 截图与标注脚本

截图统一用 headless Chrome + Playwright，直接跑 Flink Web UI 的 hash 路由，例如：

```text
http://localhost:8081/#/overview
http://localhost:8081/#/job/running/<jobId>/overview/<vertexId>/backpressure
http://localhost:8081/#/task-manager/<taskManagerId>/thread-dump
```

约定：

```text
viewport  1920 宽，按页面需要加高（长页面用 1500 / 1800），保证整页可见
标注      只有红色方框 + 箭头 + 中文说明，不使用任何填充色
标号      只写数字（红字 + 白描边），不用实心圆
```

标注脚本用 Pillow 把上述元素画在真实截图之上；
坐标既支持绝对像素，也支持「按页面文字锚定 + 偏移」，
所以页面布局变化时只需重新导出元素坐标即可，不必手工改坐标。
