package org.learning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_从文件中读取数据 {

    public static void main(String[] args) throws Exception {
        // 1. 创建Flink的流处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 获取资源文件路径
        String filePath = Flink01_从文件中读取数据.class.getClassLoader().getResource("wordcount").getPath();

        // 3. 从文件中读取数据并注册为数据流
        DataStream<String> text = env.readTextFile(filePath);

        // 4. 对数据进行WordCount处理
        DataStream<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new Tokenizer()) // 将每行文本拆分为单词
                .keyBy(value -> value.f0) // 按单词分组
                .sum(1);                  // 统计每个单词的数量

        // 5. 打印结果到控制台
        wordCounts.print();

        // 6. 执行流处理任务
        env.execute("WordCount from File (Streaming)");
    }

    // 自定义FlatMap函数，用于将每行文本拆分为单词
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 将每行文本按空格分割为单词
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}










