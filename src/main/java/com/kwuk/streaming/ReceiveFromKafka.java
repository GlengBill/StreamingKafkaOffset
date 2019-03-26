package com.kwuk.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 实现sparkStreaming消费kafka数据
 * kafka topic(__consumer_offsets)保存获取offset
 *
 * @author Charlie Kwuk
 */
public class ReceiveFromKafka {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("ReceiveFromKafka")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        //kafka
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "test");
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("session.timeout.ms", "30000");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //topic
        Set<String> topicSet = new HashSet<String>();
        topicSet.add("media");
        topicSet.add("clientlog");

        //direct获取kafka数据
        //获取上次消费offset信息
        JavaInputDStream<ConsumerRecord<String, String>> directStream
                = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams, getLastOffsets(kafkaParams, topicSet)));


        final AtomicReference<OffsetRange[]> atomicReference = new AtomicReference<>();

        JavaDStream<ConsumerRecord<String, String>> transform
                = directStream.transform(new Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> value) throws Exception {

                OffsetRange[] offsetRanges = ((HasOffsetRanges) value.rdd()).offsetRanges();
                atomicReference.set(offsetRanges);
                return value;
            }
        });

        transform.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {

                System.out.println(consumerRecordJavaRDD.count() + " ***");
                System.out.println(atomicReference.get().length);
                OffsetRange[] offsetRanges = atomicReference.get();
                for (OffsetRange offsetRange : offsetRanges) {

                    System.out.println(" --- ");
                    System.out.println(offsetRange.topic());
                    System.out.println(offsetRange.partition());
                    System.out.println(offsetRange.fromOffset());
                    System.out.println(offsetRange.untilOffset());
                }

//                 手动提交offset ，前提是禁止自动提交
                ((CanCommitOffsets) directStream.inputDStream()).commitAsync(offsetRanges);
            }
        });


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jssc.stop();
    }

    /**
     * 获取当前topic及消费者组上次消费的offset信息
     *
     * @param kafkaParams
     * @param topics
     * @return
     */
    private static Map<TopicPartition, Long> getLastOffsets(Map<String, Object> kafkaParams, Set<String> topics) {

        Properties properties = new Properties();
        properties.putAll(kafkaParams);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        Map<TopicPartition, Long> resultMap = new HashMap<TopicPartition, Long>();
        for (String topic : topics) {

            //分配
            List<PartitionInfo> infos = consumer.partitionsFor(topic);
            List<TopicPartition> tpList = new ArrayList<TopicPartition>();
            for (PartitionInfo info : infos) {

                tpList.add(new TopicPartition(info.topic(), info.partition()));
            }
            consumer.assign(tpList);

            //获取
            for (PartitionInfo info : infos) {

                resultMap.put(new TopicPartition(info.topic(), info.partition()),
                        consumer.position(new TopicPartition(info.topic(), info.partition())));
            }
        }
        consumer.close();

        System.out.println(resultMap);
        return resultMap;
    }
}