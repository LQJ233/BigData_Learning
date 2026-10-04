#!/usr/bin/env python3
"""Spark 文档用的示意图（架构图 / 流程图 / 原理图）。"""
import os
import sys
sys.path.insert(0, "/tmp/sparkui")
from mkdiag import (Svg, INK, MUTED, LINE, BLUE, GREEN, RED, AMBER, PURPLE,
                    BLUE_BG, GREEN_BG, RED_BG, AMBER_BG, PURPLE_BG, SLATE_BG,
                    hflow, vflow)

OUT = "/tmp/sparkui/diag"


def spark_arch():
    s = Svg(1240, 640)
    s.text(620, 34, "Spark 运行时架构：Driver + Cluster Manager + Executor", size=19, weight="700")

    s.group(40, 70, 360, 340, "Driver（你的 main 方法）", tone="blue")
    s.box(70, 118, 300, 70, ["SparkContext / SparkSession", "整个应用的入口"], tone="blue")
    s.box(70, 200, 300, 70, ["DAGScheduler", "把 RDD 血缘切成 Stage"], tone="blue")
    s.box(70, 282, 300, 70, ["TaskScheduler", "把 Task 分发到 Executor"], tone="blue")
    s.box(70, 356, 300, 46, ["Web UI: :4040"], tone="gray")

    s.group(460, 70, 300, 340, "Cluster Manager", tone="amber")
    s.box(490, 118, 240, 60, ["Standalone / YARN / K8s"], tone="amber")
    s.box(490, 196, 240, 60, ["Worker / NodeManager"], tone="amber")
    s.box(490, 274, 240, 60, ["给 Driver 分配 Executor"], tone="amber")
    s.box(490, 352, 240, 46, ["资源调度"], tone="amber")

    s.group(820, 70, 380, 340, "Executor（JVM 进程，可多个）", tone="green")
    for i in range(3):
        s.box(848, 118 + i * 96, 324, 78,
              [f"Executor {i+1}", "Core 1..N：每个 Core 跑一个 Task",
               "内存：Execution + Storage"], tone="green", title_size=13.5)

    s.arrow(406, 160, 486, 140, label="申请资源")
    s.arrow(760, 200, 818, 160, color=GREEN, label="启动 Executor")
    s.arrow(406, 316, 880, 480, label="分发 Task / 收集结果")
    s.arrow(880, 500, 406, 336, color=PURPLE, label="Task 状态回报")

    s.note(40, 430, 1160, 190,
           ["三句话记住",
            "• Driver 是大脑：解析程序、切 Stage、调度 Task、维护 Web UI",
            "• Cluster Manager 是劳务市场：只负责给资源，不参与计算",
            "• Executor 是工人：真正跑 Task 的 JVM，一个 Executor 里有多少 Core 就能并行跑多少个 Task",
            "",
            "本地模式（local[*]）没有 Cluster Manager，Executor 就是 Driver 自己所在的 JVM，",
            "所以 Web UI 的 Executors 页签只会看到一行 driver——这是正常的，不是配置错了。"], tone="amber")
    s.save(f"{OUT}/spark-arch.svg")


def spark_submit_modes():
    s = Svg(1240, 600)
    s.text(620, 34, "spark-submit 的两种部署模式：client 与 cluster", size=19, weight="700")

    s.group(40, 70, 560, 300, "client 模式（默认）", tone="blue")
    s.box(70, 130, 200, 70, ["Driver", "在提交机器上"], tone="blue")
    s.box(320, 130, 250, 70, ["Executor", "在集群里"], tone="green")
    s.arrow(276, 165, 314, 165)
    s.box(70, 250, 500, 90, ["优点：日志直接在终端，适合调试和交互式开发",
                             "缺点：提交机器要一直等着，网络断了作业就断"], tone="gray", title_size=13.5)

    s.group(640, 70, 560, 300, "cluster 模式（生产常用）", tone="green")
    s.box(670, 130, 200, 70, ["Driver", "在集群里"], tone="green")
    s.box(920, 130, 250, 70, ["Executor", "在集群里"], tone="green")
    s.arrow(876, 165, 914, 165)
    s.box(670, 250, 500, 90, ["优点：提交完就能关终端，Driver 与 Executor 同机房",
                             "缺点：日志要去 YARN / Spark UI 上看"], tone="gray", title_size=13.5)

    s.note(40, 400, 1160, 170,
           ["Web UI 在哪里",
            "• client 模式：Driver 在本地，直接访问 http://localhost:4040",
            "• cluster 模式：Driver 在集群里，从 YARN ResourceManager / Spark Master 页面点进 Application 跳转",
            "• 作业结束之后 UI 就没了，要回看历史需要开启 EventLog + History Server"], tone="amber")
    s.save(f"{OUT}/spark-submit-modes.svg")


def spark_data_structures():
    s = Svg(1240, 560)
    s.text(620, 34, "RDD / DataFrame / Dataset 的关系", size=19, weight="700")

    s.box(60, 120, 330, 150, ["RDD[T]", "分布式对象集合", "强类型、无 schema", "算子：map / reduceByKey …"],
          tone="blue")
    s.box(455, 120, 330, 150, ["DataFrame = Dataset[Row]", "带 schema 的表", "有列名与类型", "算子：select / groupBy …"],
          tone="green")
    s.box(850, 120, 330, 150, ["Dataset[T]", "强类型的 DataFrame", "编译期类型检查", "Scala / Java 可用"],
          tone="purple")

    s.arrow(396, 175, 449, 175, label=".toDF()")
    s.arrow(449, 220, 396, 220, label=".rdd")
    s.arrow(791, 175, 844, 175, label=".as[T]")
    s.arrow(844, 220, 791, 220, label=".toDF()")

    s.note(60, 310, 1120, 110,
           ["选型建议",
            "• 能用 DataFrame / Dataset 就别用 RDD：Catalyst 优化器与 Tungsten 编解码只对它们生效",
            "• 需要精确控制每一行对象的处理、或用到 RDD 独有算子（如 mapPartitionsWithIndex）时才落回 RDD"], tone="amber")

    s.note(60, 440, 1120, 100,
           ["注意：RDD 与 DataFrame 互转不是免费的",
            "RDD → DataFrame 需要推导 schema；DataFrame → RDD 会把 Row 反序列化成对象，两者都会丢掉优化机会。"], tone="red")
    s.save(f"{OUT}/spark-data-structures.svg")


def spark_lazy_lineage():
    s = Svg(1240, 560)
    s.text(620, 34, "惰性求值：转换算子只记血缘，行动算子才真正执行", size=19, weight="700")

    s.group(40, 70, 1160, 180, "① 转换算子（Transformation）：只记录，不执行", tone="blue")
    hflow(s, [["textFile", "读数据"], ["filter", "过滤"], ["map", "映射"],
              ["reduceByKey", "聚合（有 shuffle）"]],
          70, 120, 240, 90, gap=44, tone="blue", start=None)
    s.text(620, 236, "这一刻没有任何 Task 被提交，Web UI 的 Jobs 页签也还是空的。", size=13.5, color=MUTED)

    s.group(40, 280, 1160, 250, "② 行动算子（Action）：向前回溯血缘，生成并执行 Job", tone="green")
    s.box(80, 336, 230, 80, ["count()", "触发 Action"], tone="green")
    s.box(380, 336, 230, 80, ["DAGScheduler", "按宽依赖切 Stage"], tone="green")
    s.box(680, 336, 230, 80, ["TaskScheduler", "分发 Task"], tone="green")
    s.box(980, 336, 180, 80, ["Executor", "执行 Task"], tone="green")
    for i in range(3):
        s.arrow(316 + i * 300, 376, 374 + i * 300, 376)
    s.text(620, 456, "反推血缘：count ← reduceByKey ← map ← filter ← textFile", size=13.5, color=INK)
    s.text(620, 492, "每个行动算子 = 一个 Job；血缘被切成的每一段 = 一个 Stage。", size=13.5, color=RED, weight="600")
    s.save(f"{OUT}/spark-lazy-lineage.svg")


def spark_narrow_wide():
    s = Svg(1240, 600)
    s.text(620, 34, "窄依赖 vs 宽依赖：是否需要 Shuffle", size=19, weight="700")

    s.group(40, 70, 560, 420, "窄依赖 Narrow Dependency（不 shuffle）", tone="green")
    s.text(320, 122, "父 RDD 的每个分区最多被一个子分区使用", size=13, color=MUTED)
    for i in range(3):
        s.rect(90 + i * 165, 150, 130, 54, fill=BLUE_BG, stroke=BLUE, rx=8)
        s.text(155 + i * 165, 183, f"父分区 {i}", size=12.5)
        s.rect(90 + i * 165, 260, 130, 54, fill=GREEN_BG, stroke=GREEN, rx=8)
        s.text(155 + i * 165, 293, f"子分区 {i}", size=12.5)
        s.arrow(155 + i * 165, 208, 155 + i * 165, 256, color=GREEN)
    s.text(320, 360, "map / filter / flatMap / union", size=14, weight="600")
    s.text(320, 396, "可以流水线执行，同一个 Stage", size=13, color=MUTED)
    s.text(320, 440, "不需要跨节点搬数据", size=13.5, color=GREEN, weight="600")

    s.group(640, 70, 560, 420, "宽依赖 Wide Dependency（要 shuffle）", tone="red")
    s.text(920, 122, "一个子分区依赖多个父分区，必须跨节点重分布", size=13, color=MUTED)
    for i in range(3):
        s.rect(690 + i * 165, 150, 130, 54, fill=BLUE_BG, stroke=BLUE, rx=8)
        s.text(755 + i * 165, 183, f"父分区 {i}", size=12.5)
        s.rect(690 + i * 165, 260, 130, 54, fill=RED_BG, stroke=RED, rx=8)
        s.text(755 + i * 165, 293, f"子分区 {i}", size=12.5)
    for a in range(3):
        for b in range(3):
            s.arrow(755 + a * 165, 208, 755 + b * 165, 256, color=RED, sw=1)
    s.text(920, 360, "groupByKey / reduceByKey / join", size=14, weight="600")
    s.text(920, 396, "被切分成两个 Stage，中间落盘", size=13, color=MUTED)
    s.text(920, 440, "Stage 边界 = Shuffle 边界", size=13.5, color=RED, weight="600")
    s.save(f"{OUT}/spark-narrow-wide.svg")


def spark_job_stage_task():
    s = Svg(1240, 600)
    s.text(620, 34, "Job → Stage → Task 三层结构", size=19, weight="700")

    s.box(60, 90, 300, 120, ["Action", "count() / collect() / saveAsTextFile()"],
          tone="purple", title_size=14)
    s.arrow(366, 150, 430, 150)
    s.box(436, 90, 300, 120, ["Job", "一个 Action 对应一个 Job", "Web UI 的 Jobs 页签"],
          tone="blue", title_size=14)
    s.arrow(742, 150, 806, 150)
    s.box(812, 90, 368, 120, ["Stage", "按宽依赖切分", "Web UI 的 Stages 页签"],
          tone="green", title_size=14)

    s.group(60, 250, 1120, 150, "Stage 内部：按分区切出 Task（一个分区一个 Task）", tone="amber")
    for i in range(6):
        s.rect(100 + i * 180, 310, 150, 66, fill=AMBER_BG, stroke=AMBER, rx=8)
        s.text(175 + i * 180, 340, f"Task {i}", size=13, weight="600")
        s.text(175 + i * 180, 362, "跑在一个 Core 上", size=11.5, color=MUTED)

    s.note(60, 430, 1120, 150,
           ["换算关系",
            "• Task 数 = 该 Stage 最后一个 RDD 的分区数",
            "• 并行度 = 同时能跑的 Task 数 = 所有 Executor 的 Core 总数",
            "• 8 个分区 + 4 个 Core → 分两轮跑完，Web UI 上能看到 Task 的启动时间错开",
            "• 数据倾斜的本质：**某些 Task 处理的数据量远大于其它 Task**，而不是 Task 数不对"], tone="amber")
    s.save(f"{OUT}/spark-job-stage-task.svg")


def spark_shuffle_why():
    s = Svg(1240, 520)
    s.text(620, 34, "为什么必须 Shuffle：把相同 Key 拉到同一个节点", size=19, weight="700")

    s.box(60, 120, 220, 260, ["Map 端输出", "分区 0：a, b, c", "分区 1：b, d", "分区 2：a, c, d"],
          tone="blue", title_size=15)
    s.arrow(286, 250, 420, 250, label="按 Key 重新分区")
    s.box(426, 120, 220, 260, ["Shuffle Write", "写到本地磁盘", "按 target 分区组织"],
          tone="amber", title_size=15)
    s.arrow(652, 250, 786, 250, label="跨节点拉取")
    s.box(792, 120, 390, 260, ["Reduce 端", "reduce 分区 0 ⟵ a, a（来自所有 Map）",
                              "reduce 分区 1 ⟵ b, b", "reduce 分区 2 ⟵ c, c", "reduce 分区 3 ⟵ d, d"],
          tone="green", title_size=14)

    s.note(60, 400, 1120, 100,
           ["Shuffle 的代价",
            "• 磁盘 IO：Map 端落盘 + Reduce 端读取",
            "• 网络 IO：Reduce 端要跨节点拉取",
            "• 序列化 / 反序列化：每个 Key 都要走一遍",
            "所以「能避免 Shuffle 就避免」是 Spark 优化的第一原则。"], tone="red")
    s.save(f"{OUT}/spark-shuffle-why.svg")


def spark_shuffle_sort():
    s = Svg(1240, 640)
    s.text(620, 34, "SortShuffleManager（Spark 3.x 唯一实现）", size=19, weight="700")

    s.group(40, 70, 1160, 250, "Map 端：写内存缓冲 → 溢写 → 合并成一个文件", tone="blue")
    hflow(s, [["写入内存缓冲", "默认 32KB，按分区+key 排序"],
              ["溢写 spill", "缓冲满就落盘（多次）"],
              ["合并 merge", "多次溢写合并成 1 个文件"],
              ["写索引文件", "index 记录每个分区的偏移"]],
          60, 130, 250, 110, gap=40, tone="blue", start=1)
    s.text(620, 292, "map 端最终产物：1 个 data 文件 + 1 个 index 文件（每个 Map Task 各一套）",
           size=13.5, color=INK, weight="600")

    s.group(40, 350, 1160, 250, "Reduce 端：按索引拉取自己那一份", tone="green")
    hflow(s, [["读 index 文件", "知道自己要哪一段"],
              ["Fetch", "跨节点拉取对应分区数据"],
              ["归并排序", "多个 Map 的段合并有序"],
              ["处理并输出", "交给 reduce 函数"]],
          60, 410, 250, 110, gap=40, tone="green", start=None)
    s.note(60, 540, 1120, 60,
           ["对比 HashShuffleManager：Map 端每个 Task 为每个 Reduce 分区各写一个文件，文件数 = map 数 × reduce 数，"
            "小文件爆炸。SortShuffle 把它们合并成 2 个文件。"], tone="amber")
    s.save(f"{OUT}/spark-shuffle-sort.svg")


def spark_shuffle_bypass():
    s = Svg(1240, 560)
    s.text(620, 34, "bypass 机制：什么时候不排序", size=19, weight="700")

    s.box(60, 120, 520, 200, ["触发条件", "• reduce 分区数 ≤ spark.shuffle.sort.bypassMergeThreshold（默认 200）",
                              "• 且不是聚合类算子", "• 且没有 map 端预聚合"], tone="blue", title_size=15)
    s.box(660, 120, 520, 200, ["做法", "• 不排序，直接按分区写多个小文件",
                               "• 最后合并成 1 个 data 文件 + 1 个 index 文件",
                               "• 省略排序开销"], tone="green", title_size=15)
    s.arrow(586, 220, 654, 220)

    s.note(60, 350, 1120, 170,
           ["什么时候受益",
            "• reduce 分区数少（小作业），排序成本反而比收益大",
            "• 只是把数据按分区重分布，不需要有序"],
           tone="amber")
    s.save(f"{OUT}/spark-shuffle-bypass.svg")


def spark_shuffle_history():
    s = Svg(1240, 620)
    s.text(620, 34, "Shuffle 实现的演进：Hash → Sort", size=19, weight="700")

    s.group(40, 70, 560, 300, "HashShuffleManager（Spark 1.2 之前）", tone="red")
    s.text(320, 122, "Map 端每个 Task 为每个 Reduce 分区写一个文件", size=13, color=MUTED)
    s.box(80, 150, 220, 100, ["Map Task 1", "reduce 0 / 1 / 2 各一个文件"], tone="red", title_size=13.5)
    s.box(340, 150, 220, 100, ["Map Task 2", "reduce 0 / 1 / 2 各一个文件"], tone="red", title_size=13.5)
    s.text(320, 290, "文件数 = Map 数 × Reduce 分区数", size=14, color=RED, weight="700")
    s.text(320, 322, "1000 × 1000 = 100 万个小文件 → 磁盘与内存双重压力", size=13, color=INK)

    s.group(640, 70, 560, 300, "SortShuffleManager（Spark 1.2 之后，2.0 起唯一）", tone="green")
    s.text(920, 122, "Map 端先排序合并，每个 Task 只产出 2 个文件", size=13, color=MUTED)
    s.box(680, 150, 220, 100, ["Map Task 1", "data + index"], tone="green", title_size=13.5)
    s.box(940, 150, 220, 100, ["Map Task 2", "data + index"], tone="green", title_size=13.5)
    s.text(920, 290, "文件数 = Map 数 × 2", size=14, color=GREEN, weight="700")
    s.text(920, 322, "1000 × 2 = 2000 个文件，减少 3 个数量级", size=13, color=INK)

    s.note(40, 400, 1160, 170,
           ["结论",
            "• Spark 3.x 只有 SortShuffleManager，Hash 实现已被移除",
            "• 所以「shuffle 文件数过多」在现代 Spark 里主要表现为 Reduce 分区数过大、或并行度过高",
            "• 真正要调的是 spark.sql.shuffle.partitions / spark.default.parallelism，而不是换 ShuffleManager"], tone="amber")
    s.save(f"{OUT}/spark-shuffle-history.svg")


def spark_skew_why():
    s = Svg(1240, 600)
    s.text(620, 34, "数据倾斜的本质：少数 Task 处理了绝大多数数据", size=19, weight="700")

    s.group(40, 70, 1160, 230, "① Key 分布不均 → 分区数据量不均", tone="red")
    keys = [("hot-key", 900), ("user-1", 30), ("user-2", 25), ("user-3", 20), ("user-4", 15), ("user-5", 10)]
    x = 90
    for name, v in keys:
        h = v * 0.18
        s.rect(x, 250 - h, 150, h, fill=RED_BG if name == "hot-key" else BLUE_BG,
               stroke=RED if name == "hot-key" else BLUE, rx=6)
        s.text(x + 75, 268, name, size=12, color=RED if name == "hot-key" else INK)
        s.text(x + 75, 286, str(v), size=11.5, color=MUTED)
        x += 175

    s.group(40, 330, 1160, 250, "② 同一个 Stage 里 Task 耗时悬殊", tone="amber")
    bars = [("Task 0", 40), ("Task 1", 42), ("Task 2", 38), ("Task 3", 100)]
    x = 100
    for name, v in bars:
        w = max(40, v * 2)
        s.rect(x, 400, w, 60, fill=RED_BG if v == 100 else GREEN_BG,
               stroke=RED if v == 100 else GREEN, rx=6)
        s.text(x + w / 2, 436, f"{name}", size=12.5, color=INK if v < 100 else RED, weight="600")
        s.text(x + w / 2, 486, f"{v}s", size=12.5, color=RED if v == 100 else MUTED)
        x += 280

    s.text(620, 546, "其它 Task 早早跑完在等它 —— 整个 Stage 的耗时由最慢的 Task 决定。",
           size=14, color=RED, weight="700")
    s.save(f"{OUT}/spark-skew-why.svg")


def spark_skew_2stage():
    s = Svg(1240, 620)
    s.text(620, 34, "方案一：两阶段聚合（加随机前缀再打散）", size=19, weight="700")

    s.group(40, 70, 1160, 220, "① 第一阶段：加随机前缀，把热点 Key 拆成 N 份", tone="blue")
    s.box(70, 120, 240, 100, ["原始数据", "hot-key: 100 万条"], tone="red", title_size=13.5)
    s.arrow(316, 170, 390, 170)
    s.box(396, 120, 300, 100, ["加前缀", "hot-key_0 … hot-key_9"],
          tone="blue", title_size=13.5)
    s.arrow(702, 170, 776, 170)
    s.box(782, 120, 380, 100, ["局部聚合", "10 个 Task 各处理 10 万条"],
          tone="green", title_size=13.5)

    s.group(40, 320, 1160, 230, "② 第二阶段：去掉前缀，再聚合一次", tone="blue")
    s.box(70, 380, 300, 100, ["去前缀", "hot-key_0 … hot-key_9 → hot-key"], tone="blue", title_size=13.5)
    s.arrow(376, 430, 450, 430)
    s.box(456, 380, 300, 100, ["全局聚合", "10 个局部结果相加"], tone="green", title_size=13.5)
    s.arrow(762, 430, 836, 430)
    s.box(842, 380, 320, 100, ["最终结果", "hot-key: 100 万"], tone="green", title_size=13.5)

    s.note(40, 566, 1160, 0, [""], tone="gray")
    s.text(620, 580, "注意：只对确认倾斜的 Key 加前缀；求和 / 计数可以直接用，去重要改造。",
           size=13.5, color=RED, weight="600")
    s.save(f"{OUT}/spark-skew-2stage.svg")


def spark_skew_mapjoin():
    s = Svg(1240, 560)
    s.text(620, 34, "方案二：Reduce Join 换成 Map Join（广播小表）", size=19, weight="700")

    s.group(40, 70, 560, 300, "Reduce Join（默认）", tone="red")
    s.box(70, 130, 220, 90, ["大表 orders", "按 user_id 分区"], tone="blue", title_size=13.5)
    s.box(340, 130, 220, 90, ["小表 users", "按 user_id 分区"], tone="blue", title_size=13.5)
    s.arrow(290, 220, 420, 262, color=RED, label="两边都要 shuffle")
    s.arrow(450, 220, 420, 262, color=RED)
    s.box(230, 268, 220, 80, ["按 Key 拉取", "热点 Key 集中"], tone="red", title_size=13.5)
    s.text(320, 372, "热点的 Key 会把某个 Reduce Task 打爆", size=12.5, color=RED)

    s.group(640, 70, 560, 300, "Map Join / Broadcast Join", tone="green")
    s.box(670, 130, 220, 90, ["大表 orders", "不动，不 shuffle"], tone="green", title_size=13.5)
    s.box(940, 130, 220, 90, ["小表 users", "广播到每个 Executor"], tone="amber", title_size=13.5)
    s.arrow(940, 224, 890, 262, color=AMBER, label="Broadcast")
    s.box(800, 268, 300, 80, ["每个 Task 本地查哈希表", "没有任何 shuffle"], tone="green", title_size=13.5)
    s.text(920, 372, "热点 Key 不再集中：每个 Task 只处理自己的那部分", size=12.5, color=GREEN)

    s.note(40, 400, 1160, 130,
           ["怎么用",
            "• SQL：`SELECT /*+ BROADCAST(u) */ ... FROM orders o JOIN users u ON ...`",
            "• DataFrame：`broadcast(userDf)`",
            "• 阈值：`spark.sql.autoBroadcastJoinThreshold` 默认 10MB；小表超过阈值就会退回 SortMergeJoin",
            "• 前提：被广播的那张表要真的小，否则会把 Driver / Executor 内存打爆"], tone="amber")
    s.save(f"{OUT}/spark-skew-mapjoin.svg")


def spark_skew_salt():
    s = Svg(1240, 620)
    s.text(620, 34, "方案三：采样倾斜 Key 并拆开（大表 Join 大表）", size=19, weight="700")

    hflow(s, [["① 采样", "对 Key 采样，统计出现次数"],
              ["② 找出热点", "取 Top N 作为倾斜 Key"],
              ["③ 拆大表", "热点 Key 加随机前缀（0~N）"],
              ["④ 扩容小表", "另一边把热点 Key 复制 N 份"],
              ["⑤ 分别 Join", "热点单独 Join，其余走普通 Join"],
              ["⑥ 合并", "union 两部分结果"]],
          40, 120, 180, 140, gap=24, tone="blue", start=1)

    s.note(40, 320, 1160, 130,
           ["关键点",
            "• 前缀范围 N 要 ≥ 最大并行度，否则拆完还是不够分散",
            "• 只有热点 Key 加前缀，其余数据保持不变，避免无谓的膨胀",
            "• 扩容的小表只复制热点那几行，不是整表翻 N 倍"], tone="amber")

    s.note(40, 470, 1160, 120,
           ["Spark 3.x 的更优解",
            "开启 AQE 的自动倾斜处理后，很多场景不需要手写上面的逻辑：",
            "`spark.sql.adaptive.enabled=true` + `spark.sql.adaptive.skewJoin.enabled=true`",
            "运行时发现某个分区过大时，Spark 会自动把它拆成多个子分区。详见 13 章。"], tone="green")
    s.save(f"{OUT}/spark-skew-salt.svg")


def spark_sql_pipeline():
    s = Svg(1240, 560)
    s.text(620, 34, "一条 SQL 的执行链路：Catalyst + Tungsten", size=19, weight="700")
    hflow(s, [["SQL / DataFrame", "用户写的代码"],
              ["未解析逻辑计划", "Unresolved Logical Plan"],
              ["解析 + Catalog", "补全表和列"],
              ["优化逻辑计划", "谓词下推 / 列裁剪 / 常量折叠"],
              ["物理计划", "BroadcastJoin / SortMergeJoin …"],
              ["选最优计划", "Cost Model"],
              ["RDD 执行", "代码生成 + 执行"]],
          40, 130, 150, 140, gap=20, tone="blue", start=1)
    s.note(30, 330, 1180, 100,
           ["为什么要用 DataFrame / SQL 而不是 RDD",
            "Catalyst 会重写你的计划（谓词下推、列裁剪、join 重排），Tungsten 会生成字节码并直接操作二进制数据。",
            "这两件事对 RDD 完全不生效——RDD 里写的是什么，就执行什么。"], tone="green")
    s.note(30, 450, 1180, 90,
           ["怎么看这些计划",
            "df.explain(true) 打印全部阶段；Web UI 的 SQL 页签会画出最终物理计划的 DAG。"], tone="amber")
    s.save(f"{OUT}/spark-sql-pipeline.svg")


def spark_aqe():
    s = Svg(1240, 600)
    s.text(620, 34, "AQE（自适应查询执行）：运行时的三个优化", size=19, weight="700")
    steps = [
        ["① 合并 Shuffle 分区", "把过小的分区合并", "解决分区数过多、小任务太多"],
        ["② 转换 Join 策略", "SortMergeJoin → BroadcastJoin", "运行时发现小表就转成广播"],
        ["③ 处理数据倾斜", "拆分过大的分区", "自动把倾斜 Task 拆成多个子任务"],
    ]
    y = 100
    for name, how, why in steps:
        s.box(60, y, 340, 100, [name], tone="green", title_size=15)
        s.text(430, y + 40, how, size=14, anchor="start", color=INK)
        s.text(430, y + 72, why, size=13, anchor="start", color=MUTED)
        y += 130

    s.note(60, 490, 1120, 100,
           ["开启方式（Spark 3.3 默认已开）",
            "spark.sql.adaptive.enabled = true",
            "spark.sql.adaptive.coalescePartitions.enabled = true",
            "spark.sql.adaptive.skewJoin.enabled = true"], tone="amber")
    s.save(f"{OUT}/spark-aqe.svg")


def spark_join_strategies():
    s = Svg(1240, 600)
    s.text(620, 34, "三种 Join 策略：谁 shuffle、谁广播", size=19, weight="700")

    rows = [
        ("BroadcastHashJoin", "小表广播到每个 Executor", "不 shuffle 大表，最快", "green"),
        ("ShuffleHashJoin", "两边都按 Key 分区，各自建哈希表", "分区要能放进内存", "amber"),
        ("SortMergeJoin", "两边都 shuffle 并排序，再归并", "默认策略，能处理大表", "blue"),
    ]
    y = 110
    for name, how, note, tone in rows:
        s.box(60, y, 320, 110, [name], tone=tone, title_size=14.5)
        s.text(410, y + 45, how, size=13.5, anchor="start")
        s.text(410, y + 78, note, size=13, anchor="start", color=MUTED)
        y += 130

    s.note(60, 500, 1120, 90,
           ["选择顺序：能广播就广播（BroadcastHashJoin）→ 分区放得下就用 ShuffleHashJoin → 其余用 SortMergeJoin。",
            "AQE 会在运行时自动完成前两种的转换。"], tone="amber")
    s.save(f"{OUT}/spark-join-strategies.svg")


def spark_memory_model():
    s = Svg(1240, 620)
    s.text(620, 34, "Executor 内存模型：统一内存管理", size=19, weight="700")

    s.rect(80, 90, 380, 460, fill=SLATE_BG, stroke=INK, rx=12, sw=2)
    s.text(270, 124, "Executor 进程内存", size=15, weight="700")
    s.rect(110, 150, 320, 60, fill=BLUE_BG, stroke=BLUE, rx=8)
    s.text(270, 186, "Reserved Memory（300MB 固定）", size=12.5)

    s.rect(110, 222, 320, 240, fill=GREEN_BG, stroke=GREEN, rx=8)
    s.text(270, 250, "User Memory", size=13.5, weight="700")
    s.text(270, 274, "用户数据结构 / 自定义对象", size=11.5, color=MUTED)
    s.rect(126, 292, 288, 150, fill="#ffffff", stroke=GREEN, rx=6, sw=1.4)
    s.text(270, 318, "Spark Memory（统一内存）", size=13, weight="600")
    s.rect(140, 334, 260, 46, fill=AMBER_BG, stroke=AMBER, rx=5)
    s.text(270, 362, "Execution：shuffle / join / sort", size=12)
    s.rect(140, 386, 260, 46, fill=PURPLE_BG, stroke=PURPLE, rx=5)
    s.text(270, 414, "Storage：cache / broadcast", size=12)

    s.rect(110, 474, 320, 60, fill=RED_BG, stroke=RED, rx=8)
    s.text(270, 510, "Other / JVM Overhead", size=12.5)

    s.note(510, 110, 690, 200,
           ["两个关键点",
            "• Execution 与 Storage 共享一块内存，可以互相借用",
            "• 借用规则：Storage 可以借 Execution 的，Execution 可以抢占 Storage 的（被抢占的缓存会落盘或丢弃）"], tone="amber")
    s.note(510, 330, 690, 220,
           ["排查时怎么用",
            "• Storage 页签里 RDD 只缓存了一部分 → 内存不够，被挤掉了",
            "• 频繁 GC / spill → Execution 内存不足，考虑加大 executor memory 或减少 shuffle 数据量",
            "• 不要把 executor 内存全给 Storage：shuffle 会没有内存可用"], tone="blue")
    s.save(f"{OUT}/spark-memory-model.svg")


def spark_streaming_arch():
    s = Svg(1240, 600)
    s.text(620, 34, "Spark Streaming：微批模型（DStream）", size=19, weight="700")

    s.group(40, 70, 1160, 230, "① 把连续的数据流切成一批批的 RDD", tone="blue")
    for i in range(5):
        x = 90 + i * 220
        s.rect(x, 130, 180, 90, fill=BLUE_BG, stroke=BLUE, rx=8)
        s.text(x + 90, 165, f"batch {i}", size=13, weight="600")
        s.text(x + 90, 190, "一个 RDD", size=11.5, color=MUTED)
        if i < 4:
            s.arrow(x + 186, 175, x + 214, 175, color=MUTED)
    s.text(620, 250, "每 batchInterval（如 5 秒）产生一个 RDD，串成 DStream", size=13.5, color=INK)
    s.text(620, 280, "所以 DStream 上的每个算子，实际是「对每一批 RDD 各执行一次」", size=13, color=MUTED)

    s.group(40, 330, 560, 200, "② 窗口操作 = 多个批次的 RDD 一起算", tone="green")
    s.text(320, 372, "window(10s, 5s)：每 5 秒对最近 10 秒的批次做一次聚合", size=13, color=INK)
    s.box(80, 396, 480, 110, ["注意", "窗口长度与滑动步长都必须是 batchInterval 的整数倍",
                              "否则会抛异常"], tone="amber", title_size=13.5)

    s.group(640, 330, 560, 200, "③ 与 Structured Streaming 的区别", tone="purple")
    s.text(920, 372, "DStream：微批 + RDD，秒级延迟", size=13, color=INK)
    s.text(920, 402, "Structured Streaming：增量查询 + 事件时间 + exactly-once 更强", size=12.5, color=INK)
    s.text(920, 440, "新项目优先选 Structured Streaming", size=13, color=PURPLE, weight="700")

    s.note(40, 550, 1160, 0, [""], tone="gray")
    s.text(620, 566, "Web UI 上看 Streaming 页签：批次列表、每批的处理时间与调度延迟、输入速率。",
           size=13.5, color=INK, weight="600")
    s.save(f"{OUT}/spark-streaming-arch.svg")


def spark_webui_nav():
    s = Svg(1240, 560)
    s.text(620, 34, "Spark Web UI 的 6 个页签：各看什么", size=19, weight="700")
    tabs = [
        ("Jobs", "所有 Job 列表", "总览耗时，第一站", "blue"),
        ("Stages", "所有 Stage 列表", "看 Shuffle Read/Write、Task 分布", "green"),
        ("Storage", "RDD 缓存情况", "cache/persist 是否生效", "purple"),
        ("Environment", "运行时环境与配置", "确认配置与版本", "amber"),
        ("Executors", "每个 Executor 的资源与任务", "查数据倾斜、内存、GC", "red"),
        ("SQL", "SparkSQL 的 DAG", "看物理计划与执行", "blue"),
    ]
    y = 90
    for name, what, when, tone in tabs:
        s.box(60, y, 220, 68, [name], tone=tone, title_size=15)
        s.text(310, y + 28, what, size=13.5, anchor="start", color=INK)
        s.text(310, y + 52, when, size=12.5, anchor="start", color=MUTED)
        y += 76
    s.note(620, 90, 580, 190,
           ["下钻路径",
            "Jobs → 点 Job → 看这个 Job 的 Stage 列表",
            "Stages → 点 Stage → 看 Task 时间分布",
            "Stage 详情 → 点 Task → 看 Task 的日志与指标",
            "",
            "数据倾斜就是在 Stage 详情的 Task 表里发现的。"], tone="amber")
    s.note(620, 300, 580, 190,
           ["本地模式注意",
            "local[*] 模式下 Executors 页签只有一行 driver——",
            "因为 Executor 就是 Driver 自己所在的 JVM。",
            "",
            "想看到多个 Executor，需要用 standalone / YARN 集群模式提交。"], tone="blue")
    s.save(f"{OUT}/spark-webui-nav.svg")


def spark_function_types():
    s = Svg(1240, 580)
    s.text(620, 34, "Spark SQL 函数的四种用法", size=19, weight="700")
    cols = [
        ("标量函数", ["UPPER / SUBSTR / ROUND", "一行进、一行出", "concat / coalesce / if"], "blue"),
        ("聚合函数", ["COUNT / SUM / AVG", "多行进、一行出", "常配合 GROUP BY"], "green"),
        ("窗口函数", ["ROW_NUMBER / RANK", "不折叠行，多出一列", "OVER (PARTITION BY …)"], "purple"),
        ("高阶函数", ["TRANSFORM / FILTER", "对数组 / Map 逐元素处理", "入参本身是数组"], "amber"),
    ]
    for i, (name, lines, tone) in enumerate(cols):
        x = 60 + i * 300
        s.box(x, 110, 260, 120, [name], tone=tone, title_size=16)
        for j, ln in enumerate(lines):
            s.text(x + 130, 268 + j * 30, ln, size=13, color=INK if j == 0 else MUTED)

    s.note(60, 380, 1120, 180,
           ["最容易混的两个",
            "• 聚合函数 vs 窗口函数：聚合会把多行压成一行，窗口函数保留原有行数、额外多出一列结果；",
            "  `SUM(x) OVER (...)` 是窗口函数，`SUM(x)` 配合 GROUP BY 才是聚合。",
            "• 高阶函数只作用于复杂类型：数组、Map。`TRANSFORM(arr, x -> x * 2)` 里第二个参数是 lambda。"], tone="amber")
    s.save(f"{OUT}/spark-function-types.svg")


def spark_hive_arch():
    s = Svg(1240, 600)
    s.text(620, 34, "Spark SQL 数据工程：Catalog 与 Hive 集成", size=19, weight="700")

    s.box(60, 100, 300, 90, ["SparkSession", "SQL / DataFrame"], tone="blue", title_size=14)
    s.arrow(366, 145, 430, 145, label="查表")
    s.box(436, 90, 340, 110, ["Catalog", "会话级 / 全局临时视图", "数据库与表元数据"], tone="green", title_size=14)
    s.arrow(782, 145, 846, 145, label="解析")
    s.box(852, 90, 328, 110, ["Hive Metastore", "库 / 表 / 分区元数据"], tone="amber", title_size=14)

    s.group(40, 250, 560, 200, "写出方式", tone="purple")
    s.box(70, 300, 500, 60, ["partitionBy：按列分目录（最常用）"], tone="purple", title_size=13.5)
    s.box(70, 370, 500, 60, ["bucketBy：按列分桶，配合 bucketed join"], tone="purple", title_size=13.5)

    s.group(640, 250, 560, 200, "Hive 集成的两种姿势", tone="blue")
    s.box(670, 300, 500, 60, ["enableHiveSupport()：连 Metastore，读写 Hive 表"], tone="blue", title_size=13.5)
    s.box(670, 370, 500, 60, ["spark.sql.warehouse.dir：指定数据仓库目录"], tone="blue", title_size=13.5)

    s.note(40, 470, 1160, 110,
           ["生产上的常见组合",
            "• 元数据放 Hive Metastore，数据文件放 HDFS / S3 / OSS",
            "• 小文件问题主要来自分区过细 + 每次全量覆盖写；用 partitionBy 时要控制分区基数",
            "• saveAsTable 写的是 Catalog 里的表，spark.sql(\"SELECT ...\") 才能直接查"], tone="amber")
    s.save(f"{OUT}/spark-hive-arch.svg")


def spark_roadmap():
    s = Svg(1240, 620)
    s.text(620, 34, "Spark 学习路线：15 章怎么串起来", size=19, weight="700")

    stages = [
        ("第一阶段：基础", ["01 架构与环境", "02 数据导入与数据结构"], "blue"),
        ("第二阶段：RDD 算子", ["03 转换算子", "04 行动算子"], "green"),
        ("第三阶段：DataFrame / SQL", ["05 / 06 DataFrame 算子", "09 SQL 基础", "10 语法与函数", "11 窗口与高阶函数"],
         "purple"),
        ("第四阶段：原理与调优", ["07 Shuffle 专题", "08 数据倾斜专题", "13 SQL 原理与性能"], "red"),
        ("第五阶段：工程与运维", ["12 数据工程与 Hive", "14 Spark Streaming", "15 Web UI"], "amber"),
    ]
    x = 45
    w = 210
    for name, chapters, tone in stages:
        s.box(x, 100, w, 60, [name], tone=tone, title_size=14)
        for i, ch in enumerate(chapters):
            s.box(x, 180 + i * 68, w, 56, [ch], tone="gray", title_size=12.5)
        if x + w + 26 < 1240:
            s.arrow(x + w + 4, 130, x + w + 20, 130)
        x += w + 25

    s.note(40, 540, 1160, 0, [""], tone="gray")
    s.text(620, 556, "前两个阶段打基础，第三阶段进入日常开发，第四阶段解释“为什么慢”，第五阶段面向生产。",
           size=13.5, color=INK, weight="600")
    s.save(f"{OUT}/spark-roadmap.svg")


def main():
    for fn in [spark_arch, spark_submit_modes, spark_data_structures, spark_lazy_lineage,
               spark_narrow_wide, spark_job_stage_task, spark_shuffle_why, spark_shuffle_sort,
               spark_shuffle_bypass, spark_shuffle_history, spark_skew_why, spark_skew_2stage,
               spark_skew_mapjoin, spark_skew_salt, spark_sql_pipeline, spark_aqe,
               spark_join_strategies, spark_memory_model, spark_streaming_arch, spark_webui_nav,
               spark_function_types, spark_hive_arch, spark_roadmap]:
        fn()
    print("Spark 示意图：", len(os.listdir(OUT)), "张")


if __name__ == "__main__":
    main()
