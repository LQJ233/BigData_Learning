package org.learning.basic.function;

import org.apache.flink.api.common.functions.ReduceFunction;

import org.learning.basic.Order;

/**
 * 【分组累加】把同一个 Key 上的两条记录合并成一条。
 *
 * <p>它必须配合 {@code keyBy(userId)} 使用：keyBy 先按 userId 把数据分区，
 * 之后每个 Key 上只会有一个并行实例在处理，{@link #reduce} 就会被反复调用，
 * 把"当前累加结果"和"新来的一条"合并成新的累加结果。
 *
 * <pre>
 *   u001,100  →  reduce(初始, 100)  → 总量 100
 *   u001,200  →  reduce(总量 100, 200) → 总量 300
 * </pre>
 *
 * <p>对比 SQL：它等价于 {@code SELECT user_id, SUM(amount), COUNT(*) FROM t GROUP BY user_id}
 * 的"增量执行"，只不过 Spark / Flink 是流式的，每来一条就更新一次结果，
 * 而不是等所有数据到齐再算。
 *
 * <p>用 ReduceFunction 而不是 {@code .sum("amount")} 的原因：
 * <ul>
 *   <li>可以同时累加多个字段（这里 amount 和 count 一起累加）；</li>
 *   <li>逻辑写在代码里，能单测、能打断点，不用去猜 sum() 到底做了什么；</li>
 *   <li>以后要改成"加权求和""只统计某类订单"，直接改这个方法就行。</li>
 * </ul>
 */
public class UserAmountSumFunction implements ReduceFunction<Order> {

    private static final long serialVersionUID = 1L;

    @Override
    public Order reduce(Order acc, Order current) {
        // acc     ：这个 Key 之前的累加结果
        // current ：这个 Key 新来的一条
        // 返回值 ：新的累加结果，会被 Flink 保存成"当前状态"，等下一条到来再传进来
        return new Order(
                acc.getUserId(),
                acc.getAmount() + current.getAmount(),
                acc.getCount() + current.getCount()
        );
    }
}
