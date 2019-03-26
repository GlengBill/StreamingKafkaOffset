package com.kwuk.kafka

import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.{ArrayList, HashMap, HashSet, List, Map, Properties, Set}

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{Function, VoidFunction}
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.{JavaDStream, JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions._

/**
  * 实现sparkStreaming消费kafka数据
  * kafka topic(__consumer_offsets)保存获取offset
  *
  * @author Charlie Kwuk
  */
object ReceiveFromKaf {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setMaster("local[*]").setAppName("ReceiveFromKafka").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val jssc = new JavaStreamingContext(conf, Durations.seconds(3))
    //kafka
    val kafkaParams = new util.HashMap[String, AnyRef]
    kafkaParams.put("bootstrap.servers", "localhost:9092")
    kafkaParams.put("group.id", "test")
    kafkaParams.put("enable.auto.commit", "false")
    kafkaParams.put("auto.commit.interval.ms", "1000")
    kafkaParams.put("session.timeout.ms", "30000")
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    //topic
    val topicSet = new util.HashSet[String]
    topicSet.add("media")
    topicSet.add("clientlog")

    //direct获取kafka数据
    //获取上次消费offset信息
    val directStream: JavaInputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.createDirectStream(jssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams, getLastOffsets(kafkaParams, topicSet)))

    val atomicReference = new AtomicReference[Array[OffsetRange]]
    val transform: JavaDStream[ConsumerRecord[String, String]]
    = directStream.transform(new Function[JavaRDD[ConsumerRecord[String, String]], JavaRDD[ConsumerRecord[String, String]]]() {
      @throws[Exception]
      override def call(value: JavaRDD[ConsumerRecord[String, String]]): JavaRDD[ConsumerRecord[String, String]] = {
        val offsetRanges: Array[OffsetRange] = value.rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        atomicReference.set(offsetRanges)
        value
      }
    })

    transform.foreachRDD(new VoidFunction[JavaRDD[ConsumerRecord[String, String]]]() {
      @throws[Exception]
      override def call(consumerRecordJavaRDD: JavaRDD[ConsumerRecord[String, String]]): Unit = {
        System.out.println(consumerRecordJavaRDD.count + " ***")
        System.out.println(atomicReference.get.length)
        val offsetRanges: Array[OffsetRange] = atomicReference.get
        for (offsetRange <- offsetRanges) {
          System.out.println(" --- ")
          System.out.println(offsetRange.topic)
          System.out.println(offsetRange.partition)
          System.out.println(offsetRange.fromOffset)
          System.out.println(offsetRange.untilOffset)
        }
        //                 手动提交offset ，前提是禁止自动提交
        directStream.inputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })
    jssc.start()
    try
      jssc.awaitTermination()
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
    jssc.stop()
  }

  /**
    * 获取当前topic及消费者组上次消费的offset信息
    *
    * @param kafkaParams
    * @param topics
    * @return
    */
  private def getLastOffsets(kafkaParams: util.Map[String, AnyRef], topics: util.Set[String]) = {

    val properties = new Properties
    properties.putAll(kafkaParams)
    val consumer = new KafkaConsumer[String, String](properties)
    val resultMap = new util.HashMap[TopicPartition, Long]
    import scala.collection.JavaConversions._

    for (topic <- topics) { //分配
      val infos: util.List[PartitionInfo] = consumer.partitionsFor(topic)
      val tpList = new util.ArrayList[TopicPartition]
      import scala.collection.JavaConversions._
      for (info <- infos) {
        tpList.add(new TopicPartition(info.topic, info.partition))
      }
      consumer.assign(tpList)
      //获取
      import scala.collection.JavaConversions._
      for (info <- infos) {
        resultMap.put(new TopicPartition(info.topic, info.partition), consumer.position(new TopicPartition(info.topic, info.partition)))
      }
    }
    consumer.close()
    System.out.println(resultMap)
    resultMap
  }

}
