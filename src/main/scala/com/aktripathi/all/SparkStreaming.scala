package com.aktripathi.all

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


/*
   Example for reading message from kafka 10 using spark-kafka-streaming 0.10.
   Initially it returns of type ConsumerRecord which can get
    topic - The topic this record is received from
    partition - The partition of the topic this record is received from
    offset - The offset of this record in the corresponding Kafka partition
    timestamp - The timestamp of the record.
    timestampType - The timestamp type
    checksum - The checksum (CRC32) of the full record
    serializedKeySize - The length of the serialized key
    serializedValueSize - The length of the serialized value
    key - The key of the record, if one exists (null is allowed)
    value - The record contents

    In below example, value and ingestion time is being fetched




 */

object SparkStreaming {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getRootLogger.setLevel(Level.WARN)

    /** Configures Spark. */
    lazy val conf = new SparkConf()
      .set("streaming.batch.duration.sec", "10")
      .setMaster("local[1]")
      .setAppName("spark-example")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("example")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(r => (r.value, r.timestamp()))


    println(stream.getClass)
    stream.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
