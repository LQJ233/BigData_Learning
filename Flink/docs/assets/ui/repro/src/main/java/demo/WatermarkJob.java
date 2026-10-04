package demo;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

/**
 * Watermark 演示 Job。
 *
 * 结构：
 *   fast-source (2 并行, 不 sleep)          ┐
 *                                          ├─ union ─ 分配 Watermark ─ 10s 滚动窗口
 *   slow-source (1 并行, 每条 sleep 200ms)  ┘
 *
 * 关键点：
 *   - 两路输入的事件时间推进速度不同，slow-source 的 Watermark 明显落后；
 *   - 多输入的窗口算子取所有输入 Watermark 的最小值，所以整个窗口的
 *     currentInputWatermark 会被 slow-source 拖住；
 *   - 在 Web UI 的 Metrics 页面里对比窗口算子的 currentInputWatermark，
 *     就能看到"一路输入落后拖住窗口"的典型现象。
 */
public class WatermarkJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 快源：事件时间每毫秒级推进，完全不 sleep。
        DataStream<Event> fast = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(2)
                .map(new RichMapFunction<Long, Event>() {
                    @Override
                    public Event map(Long value) {
                        return new Event(value * 100L, "k" + (value % 4), 1L);
                    }
                })
                .name("fast-source")
                .uid("fast-source")
                .disableChaining();

        // 慢源：每条记录 sleep 200ms，事件时间推进速度慢，Watermark 明显落后。
        DataStream<Event> slow = env
                .fromSequence(0L, Long.MAX_VALUE)
                .setParallelism(1)
                .map(new RichMapFunction<Long, Event>() {
                    @Override
                    public Event map(Long value) throws Exception {
                        Thread.sleep(200L);
                        return new Event(value * 100L, "k" + (value % 4), 1L);
                    }
                })
                .name("slow-source")
                .uid("slow-source")
                .disableChaining();

        WatermarkStrategy<Event> strategy = WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.ts);

        fast.union(slow)
                .assignTimestampsAndWatermarks(strategy)
                .name("assign-watermarks")
                .uid("assign-watermarks")
                .keyBy(event -> event.key)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new SumAggregate(), Types.LONG, Types.LONG)
                .name("event-time-window")
                .uid("event-time-window")
                .sinkTo(new DiscardSink<Long>())
                .name("discard-sink")
                .uid("watermark-discard-sink");

        env.execute("WatermarkJob");
    }

    /** 简单的事件 POJO。 */
    public static class Event {
        public long ts;
        public String key;
        public long value;

        public Event() {
        }

        public Event(long ts, String key, long value) {
            this.ts = ts;
            this.key = key;
            this.value = value;
        }
    }

    /** 累加窗口内的事件数量。 */
    public static class SumAggregate implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + value.value;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
