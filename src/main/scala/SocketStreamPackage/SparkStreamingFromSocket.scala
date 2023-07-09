package SocketStreamPackage

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object SparkStreamingFromSocket {
  def main(args: Array[String]): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.WARN)

    val serverName = "localhost"
    val port = "12345"

    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Streaming")
      .getOrCreate()

    val df = spark.readStream
      .format("socket")
      .option("host", serverName)
      .option("port", port)
      .load()

    val wordsDF = df.select(explode(split(df("value"),",")).alias("word"))

    val count = wordsDF.groupBy("word").count()

    val query = count.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
}