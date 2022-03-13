package com.hyj.flink.app;

import com.alibaba.fastjson.JSON;
import com.hyj.flink.domain.AccessOrigin;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 新老用户的统计分析
 */
public class OsUserCntAppV2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<AccessOrigin> cleanStream = environment.readTextFile("data/access.json")
                .map(new MapFunction<String, AccessOrigin>() {
                    @Override
                    public AccessOrigin map(String value) throws Exception {
                        // TODO...  json ==> Access

                        try {
                            return JSON.parseObject(value, AccessOrigin.class);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }

                    }
                }).filter(x -> x != null)
                .filter(new FilterFunction<AccessOrigin>() {
                    @Override
                    public boolean filter(AccessOrigin value) throws Exception {
                        return "startup".equals(value.event);
                    }
                });

        cleanStream.map(new MapFunction<AccessOrigin, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(AccessOrigin value) throws Exception {
                return Tuple2.of(value.nu, 1);
            }
        }).keyBy(x -> x.f0)
                .sum(1).print("总的新老用户:").setParallelism(1);


        /**
         * (iOS,1,38)
         * (Android,1,29)
         * (iOS,0,17)
         * (Android,0,16)
         *
         * ==>
         * (1, 67)
         * (0, 33)
         */

        environment.execute("OsUserCntAppV2");

    }
}
