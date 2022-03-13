package com.hyj.flink.app;

import com.alibaba.fastjson.JSON;
//import com.imooc.flink.domain.AccessV2;
import com.hyj.flink.domain.AccessV2;
import com.hyj.flink.kafka.FlinkUtils;
import com.hyj.flink.kafka.PKKafkaDeserializationSchema;
//import com.imooc.flink.udf.GaodeLocationMapFunctionV2;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FlinkETLApp {

    public static void main(String[] args)throws Exception {
        DataStream<Tuple2<String, String>> source = FlinkUtils.createKafkaStreamV3(args, PKKafkaDeserializationSchema.class);

        FastDateFormat format = FastDateFormat.getInstance("yyyyMMdd-HH");

        source.map(new MapFunction<Tuple2<String,String>, AccessV2>() {
            @Override
            public AccessV2 map(Tuple2<String, String> value) throws Exception {
                // 注意事项：一定要考虑解析的容错性
                try {
                    AccessV2 bean = JSON.parseObject(value.f1, AccessV2.class);
                    bean.id = value.f0;

                    long time = bean.time;
                    String[] splits = format.format(time).split("-");
                    String day = splits[0];
                    String hour = splits[1];
                    bean.day = day;
                    bean.hour = hour;

                    return bean;
                } catch (Exception e) {
                    e.printStackTrace(); // 写到某个地方
                    return null;
                }
            }
        }).filter(x -> x != null)
                .filter(new FilterFunction<AccessV2>() {
                    @Override
                    public boolean filter(AccessV2 value) throws Exception {
                        return "startup".equals(value.event);
                    }
                })
                //.map(new GaodeLocationMapFunctionV2())
                .addSink(JdbcSink.sink(
                        "insert into ch_event  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (pstmt, x)->{
                            pstmt.setString(1, x.id);
                            pstmt.setString(2, x.device);
                            pstmt.setString(3, x.deviceType);
                            pstmt.setString(4, x.os);
                            pstmt.setString(5, x.event);
                            pstmt.setString(6, x.net);
                            pstmt.setString(7, x.channel);
                            pstmt.setString(8, x.uid);
                            pstmt.setInt(9, x.nu);
                            pstmt.setString(10, x.ip);
                            pstmt.setLong(11, x.time);
                            pstmt.setString(12, x.version);
                            pstmt.setString(13, x.province);
                            pstmt.setString(14, x.city);
                            pstmt.setString(15, x.day);
                            pstmt.setString(16, x.hour);
                            pstmt.setLong(17, System.currentTimeMillis());
                        },
                        JdbcExecutionOptions.builder().withBatchSize(50).withBatchIntervalMs(4000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://hadoop000:8123/pk")
                                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                .build()
                ));;
        FlinkUtils.env.execute();
    }
}
