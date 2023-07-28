package Archives

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingFromSocketSSC {
  def main(args: Array[String]): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.WARN)

    val serverName = "localhost"
    val port = 12345

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingProject")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val socketStream = ssc.socketTextStream(serverName, port, StorageLevel.MEMORY_AND_DISK_SER)
    ssc.start()
    ssc.awaitTermination()
  }
}