package demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Checkpoint 失败演示 Job。
 *
 * 在强反压的前提下，把 Checkpoint 超时时间设成 1 秒，
 * 于是每次 Checkpoint 都会因为 Barrier 迟迟不能对齐而超时失败：
 *   - Checkpoints 页面 Failed 计数持续上涨；
 *   - 失败原因通常是 "Checkpoint expired before completing"；
 *   - 任务本身仍然 RUNNING（容忍一定次数的 Checkpoint 失败）。
 */
public class CheckpointFailJob {

    private static final int PARALLELISM = 4;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        env.enableCheckpointing(3_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        // 故意设置 1 秒超时：在强反压下必然超时。
        config.setCheckpointTimeout(1_000L);
        config.setMinPauseBetweenCheckpoints(2_000L);
        config.setMaxConcurrentCheckpoints(1);
        // 容忍失败，保证作业本身继续 RUNNING，方便观察 Failed 计数。
        config.setTolerableCheckpointFailureNumber(1_000);

        DataStream<Long> source = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(PARALLELISM)
                .name("fast-source")
                .uid("ckpt-fail-source");

        DataStream<String> keyed = source
                .map(new RichMapFunction<Long, String>() {
                    @Override
                    public String map(Long value) {
                        return "k" + (value % 32);
                    }
                })
                .name("parse-and-add-key")
                .uid("ckpt-fail-parse")
                .disableChaining();

        keyed
                .keyBy(value -> value)
                .process(new SlowAggregate(30L))
                .name("slow-keyed-agg")
                .uid("ckpt-fail-agg")
                .setParallelism(PARALLELISM)
                .sinkTo(new DiscardSink<String>())
                .name("discard-sink")
                .uid("ckpt-fail-sink")
                .setParallelism(PARALLELISM);

        env.execute("CheckpointFailJob");
    }

    /** 慢处理算子：制造反压，让 Barrier 无法及时对齐。 */
    public static class SlowAggregate extends KeyedProcessFunction<String, String, String> {

        private final long sleepMs;
        private transient ValueState<Long> count;

        public SlowAggregate(long sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public void open(OpenContext openContext) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            Thread.sleep(sleepMs);
            Long current = count.value();
            long next = (current == null ? 0L : current) + 1L;
            count.update(next);
            out.collect(value + "=" + next);
        }
    }
}
