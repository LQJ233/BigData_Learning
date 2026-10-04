package org.learning.basic.function;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * 【Demo 专用 Source】每 3 秒发一条订单，发完 5 条就结束。
 *
 * <p>为什么要专门写这个？因为 {@link UserAmountProcessFunction} 里注册的是
 * "10 秒后触发"的定时器，如果用 {@code env.fromElements(...)} 这种
 * 瞬间就发完的 Source，作业会在定时器触发之前就结束，看不到 TIMER 输出。
 *
 * <p>这个 Source 让作业存活约 15 秒，定时器才有机会触发。
 *
 * <p><b>生产环境不要这样写。</b>真实项目用 {@code KafkaSource} / CDC 等
 * FLIP-27 新 Source；这里用老的 {@link RichSourceFunction} 只是为了演示。
 *
 * <p>要点：
 * <ul>
 *   <li>{@code ctx.collect(...)} 要放在 {@code synchronized (ctx.getCheckpointLock())} 里，
 *       保证"发数据"和"做 Checkpoint"不会互相打断；</li>
 *   <li>{@code cancel()} 由 Flink 在取消作业时调用，用来让 run() 里的循环退出。</li>
 * </ul>
 */
public class SlowOrderSource extends RichSourceFunction<String> {

    private static final long serialVersionUID = 1L;

    /** 每条数据之间间隔多久（毫秒）。 */
    private final long intervalMs;

    /** volatile：cancel() 可能在另一个线程调用。 */
    private volatile boolean running = true;

    /** 演示数据：混了正常数据、脏数据、负金额。 */
    private static final String[] DATA = {
            "u001,100",
            "u001,300",
            "u002,200",
            "bad-data",
            "u003,-50",
            "u003,80"
    };

    public SlowOrderSource() {
        this(3_000L);
    }

    public SlowOrderSource(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (String record : DATA) {
            if (!running) {
                return;
            }
            // 加锁后再发数据，避免与 Checkpoint 的快照动作冲突。
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(record);
            }
            Thread.sleep(intervalMs);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
