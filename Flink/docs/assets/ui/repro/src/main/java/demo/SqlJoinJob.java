package demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * SQL 演示 Job：双流 Interval Join。
 *
 * 结构里会出现两个 Source、一个 Join 算子（两个输入边），
 * 非常适合用来展示"多输入算子的 Checkpoint 需要 Barrier 对齐"。
 */
public class SqlJoinJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5_000L);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> orders = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(4)
                .name("order-source")
                .uid("join-order-source")
                .map(new RichMapFunction<Long, Row>() {
                    @Override
                    public Row map(Long value) throws Exception {
                        Thread.sleep(3L);
                        return Row.of(value.intValue(), (int) (value % 16), LocalDateTime.now());
                    }
                })
                .returns(Types.ROW_NAMED(new String[]{"orderId", "userId", "ts"},
                        Types.INT, Types.INT, Types.LOCAL_DATE_TIME))
                .name("order-map")
                .uid("join-order-map")
                .disableChaining();

        DataStream<Row> payments = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(2)
                .name("payment-source")
                .uid("join-payment-source")
                .map(new RichMapFunction<Long, Row>() {
                    @Override
                    public Row map(Long value) throws Exception {
                        Thread.sleep(5L);
                        return Row.of((int) (value % 16), value, LocalDateTime.now());
                    }
                })
                .returns(Types.ROW_NAMED(new String[]{"userId", "amount", "ts"},
                        Types.INT, Types.LONG, Types.LOCAL_DATE_TIME))
                .name("payment-map")
                .uid("join-payment-map")
                .disableChaining();

        tEnv.createTemporaryView("orders", orders, Schema.newBuilder()
                .column("orderId", "INT").column("userId", "INT").column("ts", "TIMESTAMP(3)")
                .watermark("ts", "ts - INTERVAL '3' SECOND").build());

        tEnv.createTemporaryView("payments", payments, Schema.newBuilder()
                .column("userId", "INT").column("amount", "BIGINT").column("ts", "TIMESTAMP(3)")
                .watermark("ts", "ts - INTERVAL '3' SECOND").build());

        Table joined = tEnv.sqlQuery(
                "SELECT o.orderId, o.userId, p.amount, o.ts AS order_time "
                        + "FROM orders AS o JOIN payments AS p "
                        + "ON o.userId = p.userId "
                        + "AND o.ts BETWEEN p.ts - INTERVAL '5' SECOND AND p.ts + INTERVAL '5' SECOND");

        tEnv.toDataStream(joined)
                .map(row -> ((Row) row).toString())
                .name("join-result")
                .uid("join-result")
                .sinkTo(new DiscardSink<String>())
                .name("discard-sink")
                .uid("join-discard-sink");

        env.execute("SqlJoinJob");
    }
}
