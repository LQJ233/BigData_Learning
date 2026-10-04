package demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 数据倾斜演示 Job。
 *
 * 90% 的记录都使用同一个 Key "hot"，因此它们全部进入同一个 Subtask；
 * 剩下 10% 分散到 64 个 cold Key 上。
 *
 * 在 Web UI 的 Subtasks / Metrics 里可以看到：
 *   - 少数 Subtask 的 Records In / Busy 极高；
 *   - 其他 Subtask 基本空闲（Idle 高）。
 */
public class SkewJob {

    private static final int PARALLELISM = 4;
    private static final long SLEEP_MS = 4L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(5_000L);

        DataStream<Long> source = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(PARALLELISM)
                .name("fast-source")
                .uid("skew-fast-source");

        DataStream<String> keyed = source
                .map(new RichMapFunction<Long, String>() {
                    @Override
                    public String map(Long value) {
                        // 90% 落到同一个热点 Key，制造典型的 Key 倾斜。
                        return (value % 10 == 0) ? "cold-" + (value % 64) : "hot";
                    }
                })
                .name("add-skewed-key")
                .uid("add-skewed-key")
                .disableChaining();

        keyed
                .keyBy(value -> value)
                .process(new SlowKeyedCount(SLEEP_MS))
                .name("skewed-keyed-agg")
                .uid("skewed-keyed-agg")
                .setParallelism(PARALLELISM)
                .sinkTo(new DiscardSink<String>())
                .name("discard-sink")
                .uid("skew-discard-sink")
                .setParallelism(PARALLELISM);

        env.execute("SkewJob");
    }

    /**
     * 每条记录 sleep 一小段时间，并用 MapState 记录每个 (Key, 业务键) 的计数。
     * sleep 让热点 Subtask 达到饱和，从而同时体现 "倾斜 + 少量 Subtask 反压"。
     */
    public static class SlowKeyedCount extends KeyedProcessFunction<String, String, String> {

        private final long sleepMs;
        private transient MapState<String, Long> counts;

        public SlowKeyedCount(long sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public void open(OpenContext openContext) {
            counts = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("counts", Types.STRING, Types.LONG));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            Thread.sleep(sleepMs);
            String businessKey = value + "-" + (System.nanoTime() % 1000);
            Long current = counts.get(businessKey);
            counts.put(businessKey, current == null ? 1L : current + 1L);
            out.collect(value);
        }
    }
}
