# 示意图（SVG）说明

本目录的 SVG 用于替代原来文档里"大段 ```text 字符画"的部分，
覆盖架构图、流程图、状态机、时间线、对比图五类。

| 前缀 | 主题 | 用在哪 |
|---|---|---|
| `arch-*` | 运行时架构、Operator/Task、Shuffle、OperatorChain、KeyGroup、Barrier、快照恢复、EventTime、窗口 Timer、两阶段提交、内存模型、故障恢复 | `10-专题原理/*` |
| `ts-*` | 各类故障的判定流程（启动失败 / Checkpoint / 反压 / 重复 / 丢失 / Watermark / OOM / 重启） | `09-故障排查/*` |
| `basic-*` | 程序骨架、代码与 Web UI 的对应关系 | `00-速查.md`、`01-基础使用.md` |
| `wm-*` / `win-*` | Watermark 与窗口 | `02-时间与窗口/*` |
| `state-*` / `ttl-*` | 状态类型与 TTL | `03-State/*` |
| `kafka-*` / `semantics-*` | Kafka 并行度、两阶段提交、三档语义 | `04-数据源与Sink/*` |
| `sql-*` / `table-*` / `join-*` / `window-tvf` / `lookup-*` / `cdc-*` | SQL 计划、Join 类型、TVF、Lookup、CDC | `05-FlinkSQL/*` |
| `sc-*` | 九个生产场景的流水线 | `06-常见生产开发场景/*` |
| `perf-order` / `job-states` / `scale-decision` | 排查顺序、作业状态机、扩容决策 | `07-性能优化`、`08-生产运维` |

生成方式：`mkdiag.py` 提供方框 / 箭头 / 泳道 / 流程等图元，
各主题脚本按固定坐标绘制，并用脚本校验"文字与图形不越界"。
所有图都使用同一套配色与字体，和 Web UI 标注截图保持一致的观感。
