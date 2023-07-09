package SocketStreamPackage

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

object SparkStreamingFromSocketSSC {
  def main(args: Array[String]): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.WARN)

    val serverName = "localhost"
    val port = 12345

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Milliseconds(100))

    val lines = ssc.socketTextStream(serverName, port, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}