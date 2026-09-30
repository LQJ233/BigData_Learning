package com.Sparkcore;
//foreachPartition ()遍历 RDD 中每一个分区，对每一个分区都执行指定的函数

//public class Spark0310_行动算子_foreachPartition
//{
//    public static void main(String[] args) {
//// 1. 创建配置对象
//        SparkConf conf = new SparkConf().setAppName("core").setMaster("local[*]");
//// 2. 创建 sc 环境
//        JavaSparkContext sc = new JavaSparkContext(conf);
//// 3. 编写代码
//        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
//// 多线程一起计算 分区间无序 单个分区有序
//        //parallelize.foreachPartition(new VoidFunction<Iterator<Integer>>() {
//            @Override
//            public void call(Iterator<Integer> integerIterator) throws Exception {
//// 一次处理一个分区的数据
//                while (integerIterator.hasNext()) {
//                    Integer next = integerIterator.next();
//                    System.out.println(next);
//                }
//            }
//        });
//// 4. 关闭 sc
//        sc.stop();
//    }
//}