package org.learning.basic.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import org.learning.basic.Order;

/**
 * 【解析 + if 判断】把一行文本 {@code "u001,100"} 转换成 {@link Order} 对象。
 *
 * <p>这里继承的是 {@link RichMapFunction} 而不是普通的 MapFunction，
 * 因为它比普通 Function 多两个生命周期方法：
 * <ul>
 *   <li>{@code open()}：算子启动时调用一次，用来初始化连接、注册计数器；</li>
 *   <li>{@code close()}：算子结束时调用一次，用来释放资源。</li>
 * </ul>
 *
 * <p>注册的计数器会显示在 Web UI 的「算子 → Accumulators」里，
 * 比翻日志快得多——这正是不用 lambda 而单独写一个类的好处。
 */
public class OrderParseFunction extends RichMapFunction<String, Order> {

    private static final long serialVersionUID = 1L;

    /** transient：不参与序列化，每个并行实例自己 new 一份。 */
    private transient Counter parsedCounter;
    private transient Counter badAmountCounter;

    @Override
    public void open(Configuration parameters) {
        // 计数器名字会出现在 Web UI 上，建议起有意义的名字。
        this.parsedCounter = getRuntimeContext().getMetricGroup().counter("parsed_records");
        this.badAmountCounter = getRuntimeContext().getMetricGroup().counter("bad_amount_records");
    }

    @Override
    public Order map(String value) {
        // 1. 切分：前面已经过滤过格式，这里可以放心取下标。
        String[] parts = value.split(",");
        String userId = parts[0].trim();
        long amount = Long.parseLong(parts[1].trim());

        // 2. if 判断：金额 <= 0 视为异常单，统一按 0 处理并打一个计数器。
        //    真实项目里还可以：写 side output 分流、抛异常让作业失败、
        //    或者调用外部系统做风控——取决于业务能不能容忍脏数据。
        if (amount <= 0) {
            badAmountCounter.inc();
            amount = 0L;
        }

        parsedCounter.inc();
        return new Order(userId, amount, 1L);
    }

    @Override
    public void close() {
        // 这里没有要释放的资源。如果 open() 里建了数据库连接，就在这里关掉。
    }
}
