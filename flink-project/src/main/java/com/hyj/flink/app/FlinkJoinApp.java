package com.hyj.flink.app;

import com.hyj.flink.domain.ItemInfo;
import com.hyj.flink.domain.OrderInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 业务数据：订单、条目  存储在数据库中
 *
 * mysql --> canal --> kafka --> flink
 */
public class FlinkJoinApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // order
        SingleOutputStreamOperator<OrderInfo> orderStream =  environment.socketTextStream("ruozedata001", 9527)
                .map(new MapFunction<String, OrderInfo>() {
                    @Override
                    public OrderInfo map(String value) throws Exception {
                        String[] splits = value.split(",");

                        OrderInfo info = new OrderInfo();
                        info.orderId = splits[0].trim();
                        info.time = Long.parseLong(splits[1].trim());
                        info.money = Double.parseDouble(splits[2].trim());

                        return info;
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>(){
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.time;
                            }
                        }));



        // item
        SingleOutputStreamOperator<ItemInfo> itemStream = environment.socketTextStream("ruozedata001", 9528)
                .map(new MapFunction<String, ItemInfo>() {
                    @Override
                    public ItemInfo map(String value) throws Exception {
                        String[] splits = value.split(",");
                        ItemInfo info = new ItemInfo();
                        info.itemId = Integer.parseInt(splits[0].trim());
                        info.orderId = splits[1].trim();
                        info.time = Long.parseLong(splits[2].trim());
                        info.sku = splits[3].trim();
                        info.amount = Double.parseDouble(splits[4].trim());
                        info.money = Double.parseDouble(splits[5].trim());

                        return info;
                    }
                }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<ItemInfo>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ItemInfo>(){
                            @Override
                            public long extractTimestamp(ItemInfo element, long recordTimestamp) {
                                return element.time;
                            }
                        }));

        orderStream.print("order...");
        itemStream.print("order...");

        /**
         * TODO... 两个流的join操作
         *
         * item 作为左边， order作为右边
         * 理想化的：没有延迟
         *
         * (item, null)  ==> API 查询业务库  RichXXXFunction
         * (迟到了,....)  对应于延迟到达这种item，就丢弃了...
         *     outputtag ==> item
         *     union
         */
        itemStream.coGroup(orderStream)
                .where(x -> x.orderId) // item
                .equalTo(y -> y.orderId)  // order
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<ItemInfo, OrderInfo, Tuple2<ItemInfo, OrderInfo>>() {
                    @Override
                    public void coGroup(Iterable<ItemInfo> first, Iterable<OrderInfo> second, Collector<Tuple2<ItemInfo, OrderInfo>> out) throws Exception {

                        for (ItemInfo itemInfo : first) {
                            boolean flag = false;
                            for (OrderInfo orderInfo : second) {
                                out.collect(Tuple2.of(itemInfo,orderInfo));
                                flag = true;
                            }

                            if(!flag) {
                                out.collect(Tuple2.of(itemInfo,null));
                            }
                        }
                    }
                }).print();

        environment.execute("FlinkJoinApp");
    }
}
