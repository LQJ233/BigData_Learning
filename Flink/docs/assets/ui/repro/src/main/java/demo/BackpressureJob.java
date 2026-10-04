package demo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 反压演示 Job。
 *
 * 结构：
 *   fast-source  ->  parse-and-add-key  ->  [keyBy]  ->  slow-keyed-agg  ->  discard
 *
 * 关键点：
 *   - Source 尽可能快地产生数据；
 *   - slow-keyed-agg 每条记录 sleep {@code SLEEP_MS} 毫秒，成为真正的瓶颈；
 *   - Key 均匀分布（k0..k63），所以 4 个并行实例都会被反压；
 *   - 同时开启 Checkpoint，用来观察强反压下的 Barrier 对齐耗时。
 */
public class BackpressureJob {

    /** 每个并行实例。 */
    private static final int PARALLELISM = 4;
    /** 瓶颈算子每条记录的处理耗时。 */
    private static final long SLEEP_MS = 15L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        // 5 秒一次 Checkpoint，方便在 Web UI 里立刻看到历史记录。
        env.enableCheckpointing(5_000L);

        // ------------------------------------------------------------------
        // 1. 尽可能快的 Source：无速率限制，产生 Long 序列。
        // ------------------------------------------------------------------
        DataStream<Long> source = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(PARALLELISM)
                .name("fast-source")
                .uid("fast-source");

        // ------------------------------------------------------------------
        // 2. 轻量转换：把数值变成 64 个均匀分布的 Key。
        //    disableChaining() 让它在 JobGraph 里成为独立算子，
        //    这样反压可以清楚地显示在它身上。
        // ------------------------------------------------------------------
        DataStream<String> keyed = source
                .map(new RichMapFunction<Long, String>() {
                    @Override
                    public String map(Long value) {
                        return "k" + (value % 64);
                    }
                })
                .name("parse-and-add-key")
                .uid("parse-and-add-key")
                .disableChaining();

        // ------------------------------------------------------------------
        // 3. 真正的瓶颈：keyBy 之后每个 Key 串行处理，且每条记录 sleep。
        //    同时维护 ValueState，让 Checkpoint 有真实的 State 快照。
        // ------------------------------------------------------------------
        DataStream<String> processed = keyed
                .keyBy(value -> value)
                .process(new SlowKeyedAggregate(SLEEP_MS))
                .name("slow-keyed-agg")
                .uid("slow-keyed-agg")
                .setParallelism(PARALLELISM);

        processed
                .sinkTo(new DiscardSink<>())
                .name("discard-sink")
                .uid("discard-sink")
                .setParallelism(PARALLELISM);

        env.execute("BackpressureJob");
    }

    /**
     * 每条记录 sleep 固定毫秒数，并累加 Key 的计数到 ValueState。
     */
    public static class SlowKeyedAggregate extends KeyedProcessFunction<String, String, String> {

        private final long sleepMs;
        private transient ValueState<Long> count;

        public SlowKeyedAggregate(long sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public void open(OpenContext openContext) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            // 模拟昂贵的同步处理：慢 UDF / 慢序列化 / 慢外部调用都会产生同样效果。
            Thread.sleep(sleepMs);
            Long current = count.value();
            long next = (current == null ? 0L : current) + 1L;
            count.update(next);
            out.collect(value + "=" + next);
        }
    }
}
