package demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 故障与重启演示 Job。
 *
 * 每处理到第 {@link #FAIL_AFTER} 条记录就抛出一个假的"下游系统超时"异常，
 * 配合集群的 fixed-delay 重启策略，任务会不断经历：
 *   RUNNING -> FAILED -> RESTARTING -> RUNNING ...
 *
 * 用来在 Web UI 的 Exceptions 页面和 Overview 页面观察：
 *   - 异常类型、异常堆栈、失败的 Subtask；
 *   - 任务反复重启的状态流转。
 */
public class FailingJob {

    private static final int PARALLELISM = 2;
    private static final long SLEEP_MS = 1L;
    private static final long FAIL_AFTER = 3_000L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        DataStream<Long> source = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(PARALLELISM)
                .name("fast-source")
                .uid("fail-fast-source");

        source
                .map(new RichMapFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        Thread.sleep(SLEEP_MS);
                        if (value >= FAIL_AFTER) {
                            throw new RuntimeException(
                                    "mock failure: downstream timeout while writing record " + value);
                        }
                        return "ok-" + value;
                    }
                })
                .name("failing-map")
                .uid("failing-map")
                .sinkTo(new DiscardSink<String>())
                .name("discard-sink")
                .uid("fail-discard-sink");

        env.execute("FailingJob");
    }
}
