package com.aktripathi.all

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.OutputMode

/**
 * @author aktripathi
 */
object App {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkSession = SparkSession.builder
      .master("local[4]") // More than 1
      .appName("spark-example")
      .getOrCreate()
    //create stream from socket

    //sparkSession.sparkContext.setLogLevel("ERROR")

    val topics = "example"
    val  subscribeType = "subscribe"


    val netflow = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("value.deserializer" , classOf[StringDeserializer].getCanonicalName)
      .option("startingOffsets", "latest")
      .option("includeTimestamp", true)
      .option(subscribeType, topics)
      .load().select( col("timestamp").alias("ingestion_timestamp"), col("value").cast("string").alias("kafka_msg"))


    val query =
      netflow.writeStream
        .format("console").option("truncate","false")
        .outputMode(OutputMode.Append()).start()


    query.awaitTermination()
  }

}
