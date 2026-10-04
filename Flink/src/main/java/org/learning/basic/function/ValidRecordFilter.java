package org.learning.basic.function;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * 【过滤】把格式不合法的原始记录挡在门外。
 *
 * <p>为什么单独写成一个类而不是用 lambda？
 * <ul>
 *   <li>可以单独 new 出来做单元测试：{@code new ValidRecordFilter().filter("bad")}；</li>
 *   <li>Web UI 上算子名可以直接用类名，排查时一眼知道是哪段逻辑；</li>
 *   <li>以后要加日志、加计数器，直接在类里改，不用动主流程。</li>
 * </ul>
 *
 * <p>输入：Kafka 里的一行文本，形如 {@code u001,100}。
 * 输出：返回 true 表示保留，false 表示丢弃。
 */
public class ValidRecordFilter implements FilterFunction<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(String value) {
        // 1. 空值直接丢。
        if (value == null || value.trim().isEmpty()) {
            return false;
        }

        // 2. 必须正好是两段：userId,amount。
        String[] parts = value.split(",");
        if (parts.length != 2) {
            return false;
        }

        // 3. userId 不能为空。
        if (parts[0].trim().isEmpty()) {
            return false;
        }

        // 4. amount 必须是数字，否则丢弃（避免后面 parseLong 抛异常把作业搞挂）。
        try {
            Long.parseLong(parts[1].trim());
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}
