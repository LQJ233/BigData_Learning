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
 * SQL 演示 Job：滚动窗口聚合（Window TVF）。
 *
 * 目的是证明一件事：**SQL 最终也会编译成 DataStream 的 JobGraph**，
 * 所以 07 / 08 / 09 里所有排查手段（JobGraph、反压、Checkpoint、Metrics）
 * 对 SQL 作业一样适用。
 *
 * 结构：
 *   row-source -> [watermark] -> WindowAggregate -> Calc -> sink
 */
public class SqlWindowJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5_000L);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> events = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(4)
                .name("sequence-source")
                .uid("sql-sequence-source")
                .map(new RichMapFunction<Long, Row>() {
                    @Override
                    public Row map(Long value) throws Exception {
                        Thread.sleep(2L);
                        return Row.of(
                                value.intValue(),
                                "cat-" + (value % 8),
                                LocalDateTime.now());
                    }
                })
                .returns(Types.ROW_NAMED(new String[]{"id", "category", "ts"},
                        Types.INT, Types.STRING, Types.LOCAL_DATE_TIME))
                .name("row-source")
                .uid("sql-row-source")
                .disableChaining();

        // 声明事件时间字段与 Watermark：等价于 SQL DDL 里的 WATERMARK FOR ts AS ...
        Schema schema = Schema.newBuilder()
                .column("id", "INT")
                .column("category", "STRING")
                .column("ts", "TIMESTAMP(3)")
                .watermark("ts", "ts - INTERVAL '5' SECOND")
                .build();

        tEnv.createTemporaryView("events", events, schema);

        // Window TVF 写法：TUMBLE + GROUP BY window_start / window_end。
        Table result = tEnv.sqlQuery(
                "SELECT window_start, window_end, category, COUNT(*) AS cnt, SUM(id) AS total "
                        + "FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
                        + "GROUP BY window_start, window_end, category");

        tEnv.toDataStream(result)
                .map(row -> ((Row) row).toString())
                .name("sql-result")
                .uid("sql-result")
                .sinkTo(new DiscardSink<String>())
                .name("discard-sink")
                .uid("sql-discard-sink");

        env.execute("SqlWindowJob");
    }
}
