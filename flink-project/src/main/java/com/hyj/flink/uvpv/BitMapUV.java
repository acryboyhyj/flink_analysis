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
 * @description: 用bitmap去重的 ，三分钟触发一次
 * @author: huang
 **/

public class BitMapUV {
    public static void main(String[] args) throws Exception {

        DataStream<String> stream = FlinkUtils.createKafkaStreamV2(args, SimpleStringSchema.class);
//        stream.print();
        FlinkUtils.env.setParallelism(4);
        BitMapUVInternal(stream);
        FlinkUtils.env.execute();
    }

    private static void BitMapUVInternal(DataStream<String> stream) {
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
                .build();
        inputStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .keyBy(new KeySelector<ShoppingRecords, Tuple2<LocalDate, Long>>() {
                    @Override
                    public Tuple2<LocalDate, Long> getKey(ShoppingRecords value) throws Exception {
                        Instant instant = Instant.ofEpochMilli(value.getTs());
                        return Tuple2.of(
                                LocalDateTime.ofInstant(instant, ZoneId.of("Asia/Shanghai")).toLocalDate(),
                                value.getItemId()
                        );
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(3)))
                .process(new ProcessWindowFunctionBitMap())
                .addSink(new RedisSink<>(conf, new UvRedisSink()));

    }

    public static class ProcessWindowFunctionBitMap
            extends ProcessWindowFunction<ShoppingRecords, UserClickModel, Tuple2<LocalDate, Long>, TimeWindow> {

        private transient ValueState<Integer> pvState;
        private transient ValueState<Roaring64NavigableMap> bitMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Integer> pvStateDescriptor = new ValueStateDescriptor<>("pv", Integer.class);
            ValueStateDescriptor<Roaring64NavigableMap> bitMapStateDescriptor = new ValueStateDescriptor("bitMap"
                    , TypeInformation.of(new TypeHint<Roaring64NavigableMap>() {
            }));

            // 过期状态清除
            StateTtlConfig stateTtlConfig = StateTtlConfig
                    .newBuilder(org.apache.flink.api.common.time.Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            // 开启ttl
            pvStateDescriptor.enableTimeToLive(stateTtlConfig);
            bitMapStateDescriptor.enableTimeToLive(stateTtlConfig);

            pvState = this.getRuntimeContext().getState(pvStateDescriptor);
            bitMapState = this.getRuntimeContext().getState(bitMapStateDescriptor);

        }

        @Override
        public void process(Tuple2<LocalDate, Long> key,
                            ProcessWindowFunction<ShoppingRecords, UserClickModel, Tuple2<LocalDate, Long>, TimeWindow>.Context context,
                            Iterable<ShoppingRecords> elements, Collector<UserClickModel> out) throws Exception {
            // 当前状态的pv uv
            Integer pv = pvState.value();
            Roaring64NavigableMap bitMap = bitMapState.value();
            if (bitMap == null) {
                bitMap = new Roaring64NavigableMap();
                pv = 0;
            }

            Iterator<ShoppingRecords> iterator = elements.iterator();
            while (iterator.hasNext()) {
                pv = pv + 1;
                long uid = iterator.next().getUser_id();
                //如果userId可以转成long
                bitMap.add(uid);
            }
            // 更新pv
            pvState.update(pv);

            UserClickModel UserClickModel = new UserClickModel();

            UserClickModel.setDate(key.f0.toString());
            UserClickModel.setProduct(key.f1);
            UserClickModel.setPv(pv);
            UserClickModel.setUv(bitMap.getIntCardinality());

            out.collect(UserClickModel);
        }
    }

    public static class UvRedisSink implements RedisMapper<UserClickModel> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SADD);
        }

        @Override
        public String getKeyFromData(UserClickModel data) {
            return data.getDate();
        }

        @Override
        public String getValueFromData(UserClickModel data) {
            return data.getUv() + "";
        }
    }


}
