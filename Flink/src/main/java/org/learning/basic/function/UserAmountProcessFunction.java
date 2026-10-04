package org.learning.basic.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.learning.basic.Order;

/**
 * 【ProcessFunction 示例】带状态、带定时器的自定义处理。
 *
 * <p>它和 reduce 做的是同一件事（按用户累加金额与条数），
 * 但用法完全不同：reduce 只能"合并两条记录"，而 ProcessFunction 能拿到
 * 上下文、状态和定时器，可以表达更复杂的逻辑：
 * <ul>
 *   <li>{@code ValueState}：每个 Key 独立的"变量"，会被 Checkpoint 保存，故障恢复不丢；</li>
 *   <li>{@code ctx.timerService()}：可以注册"多少秒后触发"的定时器，
 *       用来做"用户 10 秒没有下单就输出一次汇总"这类需求；</li>
 *   <li>{@code ctx.output(...)}：可以把异常数据分流到 side output；</li>
 *   <li>{@code ctx.timestamp()} / {@code ctx.currentProcessingTime()}：拿到当前处理时间。</li>
 * </ul>
 *
 * <p>什么时候必须用它（而不是 map / reduce）：
 * <pre>
 *   要保存每个 Key 的状态        → ValueState / MapState
 *   要按时间触发（超时、去重窗口） → Timer
 *   要一条输入产出 0~N 条、或者丢弃 → out.collect() 调 0 次或多次
 *   要访问 event time / 水位线     → ctx.timestamp()
 * </pre>
 *
 * <p>注意：这里要用 {@code keyBy()} 之后的 {@code process()}，
 * 对应的类是 KeyedProcessFunction；如果流没有 keyBy，用的是 ProcessFunction，
 * 那种情况下拿不到 Key 级状态。
 */
public class UserAmountProcessFunction
        extends KeyedProcessFunction<String, Order, String> {

    private static final long serialVersionUID = 1L;

    /** 每个用户累加的总金额（Key 级状态）。 */
    private transient ValueState<Long> totalAmountState;

    /** 每个用户累计的订单条数（Key 级状态）。 */
    private transient ValueState<Long> totalCountState;

    @Override
    public void open(Configuration parameters) {
        // 声明两个 Key 级状态。状态描述符的名字一旦上线不要改，
        // 改了之后从 Checkpoint 恢复时会找不到原来的状态。
        totalAmountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalAmount", Long.class));
        totalCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalCount", Long.class));
    }

    @Override
    public void processElement(Order order, Context ctx, Collector<String> out)
            throws Exception {
        // 1. 读当前状态（首次为空，用 0 兜底）。
        Long totalAmount = totalAmountState.value() == null ? 0L : totalAmountState.value();
        Long totalCount = totalCountState.value() == null ? 0L : totalCountState.value();

        // 2. 累加并写回状态。
        totalAmount = totalAmount + order.getAmount();
        totalCount = totalCount + order.getCount();
        totalAmountState.update(totalAmount);
        totalCountState.update(totalCount);

        // 3. 注册一个"处理时间 10 秒后"的定时器（演示用）。
        //    真实场景：用户 10 秒没有新订单就结算一次。
        //    这里每次都注册会产生很多定时器，生产上要先用状态记录"是否已注册"。
        ctx.timerService().registerProcessingTimeTimer(
                ctx.timerService().currentProcessingTime() + 10_000L);

        // 4. 输出当前累计结果。注意这里没有把 order 原样带出去，
        //    而是输出一行可读文本，写 Kafka 时可以直接用。
        out.collect(ctx.getCurrentKey()
                + ",total=" + totalAmount
                + ",count=" + totalCount
                + ",stateTotal=" + totalAmountState.value());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        // 10 秒到点后触发。可以用来输出"超时未支付的订单提醒"。
        // 如果不想每次都输出，这里可以根据状态判断后再 collect。
        out.collect("TIMER@" + timestamp + " key=" + ctx.getCurrentKey()
                + " 累计金额=" + totalAmountState.value());
    }
}
