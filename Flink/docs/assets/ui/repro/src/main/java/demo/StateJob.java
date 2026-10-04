package demo;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 轻量有状态 Job：用于演示"健康的 Checkpoint 与 Savepoint"。
 *
 * 特点：
 *   - map 与 source 链接在同一个算子链里，map 的 20ms/条会真正拖慢 Source，
 *     因此不会产生持续堆积的反压；
 *   - 带 KeyedState，Checkpoint 有真实数据；
 *   - Checkpoint 每次都能快速完成，适合用来对照"慢 / 失败"的 Checkpoint。
 */
public class StateJob {

    public static void main(String[] args) throws Exception {
        // 默认并行度 4；可以通过命令行第一个参数覆盖，
        // 用于演示"从 Savepoint 恢复时修改并行度"。
        final int PARALLELISM = args.length > 0 ? Integer.parseInt(args[0]) : 4;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(5_000L);

        DataStream<Long> source = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(PARALLELISM)
                .name("throttled-source")
                .uid("state-source");

        DataStream<String> keyed = source
                .map(new RichMapFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        // 限速：整体吞吐约 200 条/秒，不会打满下游。
                        Thread.sleep(20L);
                        return "k" + (value % 16);
                    }
                })
                .name("throttled-map")
                .uid("state-map");

        keyed
                .keyBy(value -> value)
                .process(new CountProcess())
                .name("count-keyed-state")
                .uid("count-keyed-state")
                .setParallelism(PARALLELISM)
                .sinkTo(new DiscardSink<String>())
                .name("discard-sink")
                .uid("state-discard-sink")
                .setParallelism(PARALLELISM);

        env.execute("StateJob");
    }

    /** 纯 KeyedState 计数，不做 sleep，保证 Checkpoint / Savepoint 快速完成。 */
    public static class CountProcess extends KeyedProcessFunction<String, String, String> {

        private transient ValueState<Long> count;

        @Override
        public void open(OpenContext openContext) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            Long current = count.value();
            long next = (current == null ? 0L : current) + 1L;
            count.update(next);
            out.collect(value + "=" + next);
        }
    }
}
