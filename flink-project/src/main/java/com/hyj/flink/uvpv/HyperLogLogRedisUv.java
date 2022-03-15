package com.hyj.flink.uvpv;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.hyj.flink.kafka.FlinkUtils;
import com.hyj.flink.usrbehavior.bean.ShoppingRecords;
import com.hyj.flink.usrbehavior.bean.UserClickModel;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

/**
 * @program: flink_analysis
 * @description:
 * @author: huang
 **/

public class HyperLogLogRedisUv {
    public static void main(String[] args) throws Exception {

        DataStream<String> stream = FlinkUtils.createKafkaStreamV2(args, SimpleStringSchema.class);
//        stream.print();
        FlinkUtils.env.setParallelism(4);
        HyperLogLogRedisUvInternal(stream);
        FlinkUtils.env.execute();
    }

    private static void HyperLogLogRedisUvInternal(DataStream<String> stream) {
        SingleOutputStreamOperator<ShoppingRecords> inputStream = stream.map(new MapFunction<String, ShoppingRecords>() {
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

        String formatIn = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ";
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatIn);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig
                .Builder().setHost("192.168.2.180")
                .setTimeout(10000).build();
        inputStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<ShoppingRecords, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(ShoppingRecords value) throws Exception {
                        Instant instant = Instant.ofEpochMilli(value.getTs());
                        String redisKey =
                                LocalDateTime.ofInstant(instant, ZoneId.of("Asia/Shanghai")).toLocalDate().toString()
                                        + value.getItemId();

                        return Tuple2.of(redisKey, value.getUser_id());
                    }
                })
                .addSink(new RedisSink<>(conf, new PfaddSinkMapper()));

    }

    public static class PfaddSinkMapper implements RedisMapper<Tuple2<String, Long>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.PFADD);
        }

        @Override
        public String getKeyFromData(Tuple2<String, Long> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Long> data) {
            return data.f1 + "";
        }
    }
}
