package org.learning.basic;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.learning.basic.function.AmountDoubleFunction;
import org.learning.basic.function.OrderParseFunction;
import org.learning.basic.function.UserAmountSumFunction;
import org.learning.basic.function.ValidRecordFilter;

/**
 * 本地可跑 Demo：不需要 Kafka、不需要集群，直接在本机把整条链路跑一遍。
 *
 * <p>和 {@link OrderAmountJob} 的唯一区别是 Source 和 Sink：
 * 这里用 {@code env.fromElements(...)} 造几条数据，最后用 {@code print()} 打印结果。
 * <b>中间的算子完全一样</b>——所以本地验证过的逻辑，换成 Kafka 版就能直接上线。
 *
 * <p>运行：
 * <pre>
 *   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
 *   cd Flink
 *   mvn -q compile exec:java -Dexec.mainClass=org.learning.basic.OrderAmountLocalDemo
 * </pre>
 *
 * <p>造的数据里特意混了几种情况，方便观察每个 Function 的行为：
 * <pre>
 *   u001,100    正常
 *   u002,200    正常
 *   u001,300    正常（会与上面的 u001 累加）
 *   bad-data    没有逗号 → 被 ValidRecordFilter 丢掉
 *   u003,abc    amount 不是数字 → 被 ValidRecordFilter 丢掉
 *   u003,-50    金额为负 → 进入 OrderParseFunction 的 if 分支，按 0 处理
 *   u003,80     正常
 * </pre>
 *
 * <p>实际输出（并行度 1，已跑通）：
 * <pre>
 *   RESULT&gt; u001,total=200,count=1    ← 第 1 条 u001：100 × 2
 *   RESULT&gt; u002,total=400,count=1    ← 200 × 2
 *   RESULT&gt; u001,total=800,count=2    ← 第 2 条 u001 到达，累加：(100+300) × 2
 *   RESULT&gt; u003,total=0,count=1      ← -50 被 if 分支按 0 处理
 *   RESULT&gt; u003,total=160,count=2    ← 再累加 80：(0+80) × 2
 * </pre>
 *
 * <p>注意这里有 5 行输出、而不是 3 行：<b>流式的 reduce 是"每来一条就更新一次结果"</b>，
 * 所以 u001 和 u003 各输出了两次（第一次是中间结果，第二次才是最终结果）。
 * 这是流处理和批处理最大的思维差异——SQL 里的 {@code GROUP BY} 只在最后给一个结果，
 * 而 Flink 的 {@code keyBy().reduce()} 会持续输出更新。
 * 下游如果要"只保留每个 Key 的最新值"，可以再用 upsert 写入，或者用 {@code process()} 配合定时器。
 */
public class OrderAmountLocalDemo {

    public static void main(String[] args) throws Exception {

        // 第 1 步：执行环境。
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Demo 用并行度 1，输出顺序更好观察；生产按 Kafka 分区数设置。
        env.setParallelism(1);

        // 第 2 步：Source —— 用内存里的几条数据代替 Kafka。
        DataStream<String> source = env.fromElements(
                "u001,100",
                "u002,200",
                "u001,300",
                "bad-data",
                "u003,abc",
                "u003,-50",
                "u003,80"
        );

        // 第 3 步：Transformation —— 和 Kafka 版一字不差。
        SingleOutputStreamOperator<String> valid = source
                .filter(new ValidRecordFilter())
                // 并行度要比数据条数小，否则会出现"空跑"的算子实例。
                .setParallelism(1)
                .name("filter-valid")
                .uid("filter-valid");

        DataStream<Order> orders = valid
                .map(new OrderParseFunction())
                .name("parse-order")
                .uid("parse-order");

        DataStream<Order> doubled = orders
                .map(new AmountDoubleFunction(2L))
                .name("double-amount")
                .uid("double-amount");

        // 分组累加：把 "u001,100" 和 "u001,300" 合成一条。
        DataStream<Order> totalPerUser = doubled
                .keyBy(Order::getUserId)
                .reduce(new UserAmountSumFunction())
                .name("sum-by-user")
                .uid("sum-by-user");

        // 第 4 步：Sink —— Demo 直接打印到控制台（生产换成 KafkaSink / JDBC）。
        totalPerUser.print("RESULT").name("console-sink").setParallelism(1);

        // 第 5 步：提交 Job。
        env.execute("order-amount-local-demo");
    }
}
