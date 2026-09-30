package com.Sparkcore;

//该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理的结果进行排序，
// 默认为正序排列。排序后新产生的 RDD 的分区数与原 RDD 的分区数一致。Spark 的排序结果是全局有序。

//public class Spark0206_转换算子_sort排序
//{
//        public static void main(String[] args) {
//// 1.创建配置对象
//        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");
//// 2. 创建 sparkContext
//        JavaSparkContext sc = new JavaSparkContext(conf);
//// 3. 创建RDD
//        JavaRDD<Integer> integerJavaRDD = sc.parallelize(Arrays.asList(5, 8, 1, 11, 20), 2);
//// (1)泛型为以谁作为标准排序 (2) true 为正序 (3) 排序之后的分区个数
//        JavaRDD<Integer> sortByRDD = integerJavaRDD.sortBy(new Function<Integer, Integer>()
//        {
//            @Override
//            public Integer call(Integer v1) throws Exception
//            {
//                return v1;
//            }
//        }, true, 2);
//        sortByRDD. collect().forEach(System.out::println);
//// 4. 关闭 sc
//        sc.stop();
//    }
//
//}