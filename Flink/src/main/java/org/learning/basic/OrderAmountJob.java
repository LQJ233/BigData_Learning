package org.learning.basic;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.learning.basic.function.AmountDoubleFunction;
import org.learning.basic.function.OrderParseFunction;
import org.learning.basic.function.UserAmountSumFunction;
import org.learning.basic.function.ValidRecordFilter;

/**
 * 生产版模板：Kafka → 过滤 → 解析 → 计算 → 按用户分组累加 → Kafka。
 *
 * <p>这个类只负责"把算子串起来"，具体逻辑都在 {@code org.learning.basic.function} 包里：
 * <pre>
 *   ValidRecordFilter       过滤脏数据
 *   OrderParseFunction      String → Order（含 if 判断 + 计数器）
 *   AmountDoubleFunction    金额 ×2
 *   UserAmountSumFunction   分组累加（等价于 GROUP BY user_id 的 SUM / COUNT）
 * </pre>
 *
 * <p>拆开的好处：每个 Function 都能单独 new 出来跑单元测试，
 * 排查时也能通过断点直接停在出问题的那一个类里。
 *
 * <p>运行（需要有一个可用的 Kafka）：
 * <pre>
 *   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
 *   mvn -q compile exec:java -Dexec.mainClass=org.learning.basic.OrderAmountJob
 * </pre>
 * 只想本地看效果、不想装 Kafka，用 {@link OrderAmountLocalDemo}。
 */
public class OrderAmountJob {

    /** 生产环境改成真实的 Broker 地址，多个地址用逗号分隔。 */
    private static final String BOOTSTRAP_SERVERS = "kafka01:9092,kafka02:9092";

    /** 输入 Topic：原始订单。 */
    private static final String SOURCE_TOPIC = "order-topic";

    /** 输出 Topic：按用户汇总后的结果。 */
    private static final String SINK_TOPIC = "order-total-topic";

    public static void main(String[] args) throws Exception {

        // =============================================================
        // 第 1 步：创建执行环境
        // =============================================================
        // getExecutionEnvironment() 会根据运行上下文自动判断：
        // 本地直接跑 → 本地环境；提交到集群 → 集群环境。
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Job 级默认并行度：没有单独 setParallelism 的算子都用它。
        // 生产值从 Kafka 分区数起步，再结合 CPU、State、Sink 吞吐调整。
        env.setParallelism(4);

        // =============================================================
        // 第 2 步：创建 Source —— 从 Kafka 读原始数据
        // =============================================================
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId("flink-order-job")
                // earliest：从最早的数据开始；latest：只消费启动之后的新数据。
                // 开启 Checkpoint 后，恢复时会优先使用快照里的 offset。
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> source = env.fromSource(
                kafkaSource,
                // 这里不用事件时间，所以给一个不生成 Watermark 的策略。
                WatermarkStrategy.noWatermarks(),
                // 第三个参数是算子名，会显示在 Web UI 上，排查时非常有用。
                "kafka-order-source");

        // =============================================================
        // 第 3 步：Transformation —— 过滤 → 解析 → 计算 → 分组累加
        // =============================================================
        // 3.1 过滤脏数据。算子级并行度 2，只覆盖这一个算子。
        SingleOutputStreamOperator<String> valid = source
                .filter(new ValidRecordFilter())
                .setParallelism(2)
                .name("filter-valid")
                .uid("filter-valid");

        // 3.2 解析成 Order（内部有 if 判断 + 两个计数器）。
        DataStream<Order> orders = valid
                .map(new OrderParseFunction())
                .name("parse-order")
                .uid("parse-order");

        // 3.3 金额 ×2。
        DataStream<Order> doubled = orders
                .map(new AmountDoubleFunction(2L))
                .name("double-amount")
                .uid("double-amount");

        // 3.4 按 userId 分组累加。
        //     keyBy 会按 key 的哈希做一次重新分区（一次 Shuffle），
        //     之后同一个 userId 的数据都会落到同一个并行实例上。
        DataStream<Order> totalPerUser = doubled
                .keyBy(Order::getUserId)
                .reduce(new UserAmountSumFunction())
                .name("sum-by-user")
                .uid("sum-by-user");

        // =============================================================
        // 第 4 步：创建 Sink —— 结果写回 Kafka
        // =============================================================
        // Order 重写了 toString()，这里直接转成字符串写出去。
        DataStream<String> result = totalPerUser
                .map(Order::toString)
                .name("order-to-text")
                .uid("order-to-text");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(SINK_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                // AT_LEAST_ONCE：不丢，但故障恢复后可能重复，下游要能幂等。
                // EXACTLY_ONCE 需要 Kafka 事务 + 打开 Checkpoint。
                //
                // 注意方法名：本项目用的 Flink 1.15 里叫 setDeliverGuarantee，
                // 从 1.16 起官方改名成 setDeliveryGuarantee（多了一个 y）。
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Sink 并行度可以和上游不同：写 Kafka 时通常对齐目标 Topic 的分区数更划算。
        result.sinkTo(kafkaSink)
                .name("kafka-order-total-sink")
                .uid("kafka-order-total-sink")
                .setParallelism(4);

        // =============================================================
        // 第 5 步：execute 提交 Job
        // =============================================================
        // 上面所有算子到这里只是"画了一张图纸"，没有一条数据真正流动。
        // execute() 才是点火：把 JobGraph 提交给集群并开始运行。
        env.execute("order-amount-job");
    }
}
