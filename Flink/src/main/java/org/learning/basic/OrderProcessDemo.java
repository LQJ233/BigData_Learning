package org.learning.basic;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.learning.basic.function.OrderParseFunction;
import org.learning.basic.function.SlowOrderSource;
import org.learning.basic.function.UserAmountProcessFunction;
import org.learning.basic.function.ValidRecordFilter;

/**
 * ProcessFunction 版 Demo：和 {@link OrderAmountLocalDemo} 做同一件事，
 * 但用 {@code keyBy().process()} 代替 {@code keyBy().reduce()}。
 *
 * <p>为什么要单独跑一遍？因为这两个写法的能力范围完全不同：
 * <pre>
 *   reduce(ReduceFunction)         只能"两条合并成一条"，无状态能力、无定时器；
 *   process(KeyedProcessFunction)  能读 Key 级状态、能注册定时器、能一条产出多条或零条。
 * </pre>
 *
 * <p>只要需求里出现下面任意一条，就必须用 ProcessFunction：
 * <pre>
 *   "每个用户上次下单是什么时候"      → ValueState
 *   "10 秒没有下单就输出一次汇总"      → Timer
 *   "一条输入可能对应 0 条或 N 条输出" → out.collect() 调多次
 *   "需要迟到数据分流"                → side output
 * </pre>
 *
 * <p>运行：
 * <pre>
 *   mvn -q compile exec:java -Dexec.mainClass=org.learning.basic.OrderProcessDemo
 * </pre>
 *
 * <p>实际输出（已跑通，时间戳已省略）：
 * <pre>
 *   PROCESS&gt; u001,total=100,count=1,stateTotal=100   ← 第 1 条 u001，状态从 0 变 100
 *   PROCESS&gt; u001,total=400,count=2,stateTotal=400   ← 第 2 条 u001，状态累加到 400
 *   PROCESS&gt; u002,total=200,count=1,stateTotal=200
 *   PROCESS&gt; TIMER@1791097271363 key=u001 累计金额=400  ← 10 秒到点，定时器回调
 *   PROCESS&gt; u003,total=0,count=1,stateTotal=0        ← -50 走 if 分支按 0 处理
 *   PROCESS&gt; TIMER@1791097274350 key=u001 累计金额=400
 *   PROCESS&gt; u003,total=80,count=2,stateTotal=80
 *   PROCESS&gt; TIMER@1791097277326 key=u002 累计金额=200
 * </pre>
 *
 * <p>三条结论：
 * <ol>
 *   <li><b>状态是按 Key 隔离的</b>：u001 和 u002 各自累加，互不影响；</li>
 *   <li><b>定时器是异步触发的</b>：TIMER 行不跟着数据走，而是在 10 秒后自己冒出来；</li>
 *   <li><b>最后一条 u003 的定时器没触发</b>：数据发完作业就结束了，
 *       定时器还没来得及响——这正是"有界数据 + 处理时间定时器"的典型坑，
 *       生产上要用事件时间或让 Source 持续供数。</li>
 * </ol>
 *
 * <p>运行结束后如果看到一行
 * {@code Error during shutdown of MemoryExecutionGraphInfoStore via JVM shutdown hook}，
 * 那是 exec-maven-plugin 提前关闭类加载器导致的，作业本身已经跑完了，可以忽略。
 */
public class OrderProcessDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度 1：保证定时器和状态的输出顺序容易观察。
        env.setParallelism(1);

        // 用"每 3 秒发一条"的 Source，让作业活到 15 秒左右，
        // 这样 10 秒的定时器才有机会触发（用 fromElements 会瞬间跑完）。
        // 生产环境换成 KafkaSource。
        DataStream<String> source = env
                .addSource(new SlowOrderSource(3_000L))
                .name("slow-order-source");

        DataStream<String> result = source
                .filter(new ValidRecordFilter())
                .name("filter-valid")
                .map(new OrderParseFunction())
                .name("parse-order")
                // keyBy 之后再 process，才能拿到 Key 级状态与定时器。
                .keyBy(Order::getUserId)
                .process(new UserAmountProcessFunction())
                .name("user-amount-process")
                .uid("user-amount-process");

        result.print("PROCESS").name("console-sink").setParallelism(1);

        env.execute("order-process-demo");
    }
}
