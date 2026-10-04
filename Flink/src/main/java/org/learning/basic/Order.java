package org.learning.basic;

import java.io.Serializable;

/**
 * 订单业务对象（POJO）。
 *
 * <p>Flink 对 POJO 的要求（满足这三条才会走高效的 POJO 序列化器）：
 * <ul>
 *   <li>类是 public 的，并且有 public 无参构造方法；</li>
 *   <li>字段要么是 public，要么有标准的 getter / setter；</li>
 *   <li>字段类型本身也是 Flink 能识别的类型。</li>
 * </ul>
 *
 * <p>字段含义：
 * <ul>
 *   <li>{@code userId}：用户 ID，也是后面 keyBy 用的分组键；</li>
 *   <li>{@code amount}：金额；</li>
 *   <li>{@code count}：这个 Key 上累计了多少条记录（用于演示"分组计数"）。</li>
 * </ul>
 */
public class Order implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;
    private long amount;
    private long count;

    /** 无参构造方法：Flink 反序列化时要用。 */
    public Order() {
    }

    public Order(String userId, long amount) {
        this(userId, amount, 1L);
    }

    public Order(String userId, long amount, long count) {
        this.userId = userId;
        this.amount = amount;
        this.count = count;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    /** 输出成一行文本，方便 print() 和写 Kafka 时直接使用。 */
    @Override
    public String toString() {
        return userId + ",total=" + amount + ",count=" + count;
    }
}
