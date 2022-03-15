package com.hyj.flink.usrbehavior;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.hyj.flink.kafka.FlinkUtils;
import com.hyj.flink.usrbehavior.bean.PageViewCount;
import com.hyj.flink.usrbehavior.bean.ShoppingRecords;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.Selector;
import java.time.Duration;
import java.util.HashSet;

public class UV {
    static public Logger logger = LoggerFactory.getLogger(UV.class);
    public static void main(String[] args) throws Exception {

        DataStream<String> stream = FlinkUtils.createKafkaStreamV2(args, SimpleStringSchema.class);
//        stream.print();
        FlinkUtils.env.setParallelism(4);
        UvInternal(stream);
        FlinkUtils.env.execute();
    }
    /**
    * @Description: 实现3分钟频率实时计算24内的uv
     * 滚动+redis+读时聚合 代替超长的滑动窗口
    * @Param: [source]
    * @return: void
    */
    public static void UvInternal(DataStream<String> source) throws Exception {
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

        SingleOutputStreamOperator<PageViewCount> result = inputStream.filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new UvResult());
        result.print();

    }

    private static class UvResult implements AllWindowFunction<ShoppingRecords, PageViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window,
                          Iterable<ShoppingRecords> values,
                          Collector<PageViewCount> out) throws Exception {
            HashSet<Long> uid = new HashSet<>();
            for (ShoppingRecords value : values) {
                uid.add(value.getUser_id());
            }

            out.collect(new PageViewCount("uv", window.getEnd(), (long) uid.size()));
        }
    }

}