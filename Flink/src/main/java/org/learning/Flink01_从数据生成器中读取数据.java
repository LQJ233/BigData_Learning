//package org.learning;
//
//public class Flink01_从数据生成器中读取数据 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(1);
//
//        DataGeneratorSource<String> dataGeneratorSource =
//                new DataGeneratorSource<>(
//                        new GeneratorFunction<Long, String>() {
//                            @Override
//                            public String map(Long value) throws
//                                    Exception {
//                                return "Number:"+value;
//                            }
//                        },
//                        Long.MAX_VALUE,
//                        RateLimiterStrategy.perSecond(10),
//                        Types.STRING
//                );
//
//        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "datagenerator").print();
//        env.execute();
//    }
//}