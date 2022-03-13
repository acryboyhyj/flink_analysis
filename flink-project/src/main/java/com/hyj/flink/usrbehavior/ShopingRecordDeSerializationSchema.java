package com.hyj.flink.usrbehavior;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import com.hyj.flink.usrbehavior.bean.ShoppingRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class ShopingRecordDeSerializationSchema implements KafkaDeserializationSchema<Tuple2<String,String>> {
    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }

    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();

        String id = topic+"_"+partition+"_"+offset;

        System.out.println("key,value" + record.key() + " |" + record.value());
        String value = new String(record.value(), StandardCharsets.UTF_8);

        return Tuple2.of(id, value);
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
    }
}
