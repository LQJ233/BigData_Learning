package org.learning.basic.function;

import org.apache.flink.api.common.functions.MapFunction;

import org.learning.basic.Order;

/**
 * 【计算】把金额乘以 2。
 *
 * <p>这就是「A → B、一条进一条出」的典型 map：不改变数据条数，只改内容。
 * 示例业务逻辑，换成"换算汇率""加税""打折"都是同一个写法。
 *
 * <p>注意：新对象是必须的。Flink 的 map 输出会成为下游的输入，
 * 直接改入参再返回，在开启对象复用（object reuse）时会出现难以排查的串数据问题。
 */
public class AmountDoubleFunction implements MapFunction<Order, Order> {

    private static final long serialVersionUID = 1L;

    /** 倍数做成字段，方便改成 1.5 倍、3 倍，也方便单元测试。 */
    private final long times;

    public AmountDoubleFunction() {
        this(2L);
    }

    public AmountDoubleFunction(long times) {
        this.times = times;
    }

    @Override
    public Order map(Order order) {
        // 新建对象，不去改动传入的 order。
        return new Order(order.getUserId(), order.getAmount() * times, order.getCount());
    }
}
