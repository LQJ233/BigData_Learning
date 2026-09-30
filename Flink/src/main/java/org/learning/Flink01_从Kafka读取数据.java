package org.learning;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_从Kafka读取数据 {
    public static void main(String[] args) {
        //①导入相关依赖

        //②创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //③定义一个从Kafka中读取数据的源
        KafkaSource<String> kafkaSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers("hadoop102:9092")//指定Kafka集群的地址。
                        .setTopics("topic_1")//指定要消费的Kafka主题。
                        .setGroupId("atguigu")//指定消费者组ID。
                        .setStartingOffsets(OffsetsInitializer.latest())//设置从最新的偏移量开始消费。
                        .setValueOnlyDeserializer(new SimpleStringSchema())//指定反序列化器，这里使用SimpleStringSchema将Kafka的消息值解析为字符串。
                        .build();//标准写法，一定要写

        //④将Kafka数据源注册到Flink环境中。如果不注册，Flink的流处理管道缺少了输入源
        DataStreamSource<String> stream =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                        "kafka-source");
        //fromSource：从定义的KafkaSource中获取数据流。
        //WatermarkStrategy.noWatermarks()：指定水印策略，这里表示不生成任何水印
        //指定数据流名称：kafka-source（消费者组id）


        //⑤将数据流的内容输出到控制台，并在每条消息前加上前缀Kafka。
        stream.print("Kafka");


        //⑥执行程序
        try {
            env.execute();//必须加这句，否则程序不会执行
        } catch (Exception e) {
            e.printStackTrace();//打印异常堆栈信息
        }
    }
}
