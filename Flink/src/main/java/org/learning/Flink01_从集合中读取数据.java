package org.learning;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink01_从集合中读取数据 {
    public static void main(String[] args) {
        //创建环境
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        //创建一个集合
        List<Integer> data = Arrays.asList(1, 22, 3);
        //从集合中获取数据
        DataStreamSource<Integer> ds = env.fromCollection(data);
        
        ds.print();//打印数据

        /*返回：
        9> 1
        10> 22
        11> 3
        DataStream.print() 方法输出的结果格式为 subtaskIndex> element，
        其中 subtaskIndex 是子任务的索引
        Flink 会把数据拆分成多个子任务并行执行，每个子任务有唯一的索引。
        子任务索引的具体数值取决于 Flink 集群的配置和任务的并行度。*/










        try {
            env.execute();//必须加这句，否则程序不会执行
        } catch (Exception e) {
            e.printStackTrace();//打印异常堆栈信息
        }
    }
}
