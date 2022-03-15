package com.hyj.flink.usrbehavior;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.hyj.flink.kafka.FlinkUtils;
import com.hyj.flink.usrbehavior.bean.ShoppingRecords;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class TurnOverCount {
    static public Logger logger = LoggerFactory.getLogger(TurnOverCount.class);
    public static void main(String[] args) throws Exception {

        DataStream<String> stream = FlinkUtils.createKafkaStreamV2(args, SimpleStringSchema.class);
//        stream.print();
        FlinkUtils.env.setParallelism(4);
        getTurnOverCount(stream);
        FlinkUtils.env.execute();
    }


    public static void getTurnOverCount(DataStream<String> source) throws Exception {
//        source.print();

        String formatIn = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ";
        FastDateFormat format = FastDateFormat.getInstance(formatIn);


        SingleOutputStreamOperator<ShoppingRecords> inputStream = source.map(new MapFunction<String, ShoppingRecords>() {
                    @Override
                    public ShoppingRecords map(String value) throws Exception {
                        // 注意事项：一定要考虑解析的容错性
                        try {
                            ShoppingRecords bean = JSON.parseObject(value, ShoppingRecords.class);

                            return bean;
                        } catch (Exception e) {
                            e.printStackTrace(); // 写到某个地方
                            return null;
                        }
                    }
                }).filter(x -> x != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShoppingRecords>forBoundedOutOfOrderness(Duration.ofMillis(1))
                                .withTimestampAssigner((value, timestamp) -> value.getTs()));
        inputStream.print();
        SingleOutputStreamOperator<Tuple2<String, Long>> result = inputStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<ShoppingRecords, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(ShoppingRecords value) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(value -> value.f0)    // 按商品ID分组
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.milliseconds(5)))
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(1);
        result.print();

//
//        result.print("no randome key");
//        .aggregate(new ViewAggregateFunc(), new ViewSumWindowFunc());

//        //  并行任务改进，设计随机key，解决数据倾斜问题
//        SingleOutputStreamOperator<PageViewCount> pvStream =
//                dataStream.filter(data -> "pv".equals(data.getBehavior()))
//                        .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
//                            @Override
//                            public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
//                                Random random = new Random();
//                                return new Tuple2(new Integer(random.nextInt(10)), 1L);
//                            }
//                        })
//                        .keyBy(value -> value.f0)
//                        .window(TumblingEventTimeWindows.of(Time.hours(1)))
//                        .aggregate(new PageAgg(), new PageResult());
    }

    private static class ViewAggregateFunc
            implements AggregateFunction<ShoppingRecords,ViewAccumulator, ViewAccumulator> {
        @Override
        public ViewAccumulator createAccumulator() {
            return new ViewAccumulator();
        }

        @Override
        public ViewAccumulator add(ShoppingRecords value, ViewAccumulator accumulator) {
//            if  (accumulator.g)
            return null;
        }

        @Override
        public ViewAccumulator getResult(ViewAccumulator accumulator) {
            return null;
        }

        @Override
        public ViewAccumulator merge(ViewAccumulator a, ViewAccumulator b) {
            return null;
        }
    }

    private static class ViewSumWindowFunc {
    }

    private static class ViewAccumulator {
    }

    public class MyMapper extends RichMapFunction<String, String> {
        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            this.counter = getRuntimeContext()
                    .getMetricGroup()
                    .counter("myCounter");
        }

        @Override
        public String map(String value) throws Exception {
            this.counter.inc();
            return value;
        }
    }



}
