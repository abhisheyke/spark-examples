package com.aktripathi.all

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

import scala.collection.immutable.Map


object StreamingKafka08App {


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

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "127.0.0.1:9092")


    // This kafka driver is deprecated. Use kafka 10
    val p = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, "example".split(",").toSet).map(_._2)


    println(p.getClass)
    p.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
