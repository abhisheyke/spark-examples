package com.aktripathi.k8s

import com.aktripathi.all.ESConnection
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object TryK8S {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getRootLogger.setLevel(Level.WARN)

    /** Configures Spark. */
    lazy val conf = new SparkConf()
      .set("streaming.batch.duration.sec", "10")
      //.setMaster("local[1]")
      .setAppName("spark-on-k8s-example")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka:9092",
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
    ).map(r => r.value).filter( v => !Option(v).isEmpty)


    //val


    stream.foreachRDD( rdd => {
      rdd.foreachPartition( value => {

        value.foreach( v => {
           ESConnection.write("aktripathi", "test", v)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }

}
