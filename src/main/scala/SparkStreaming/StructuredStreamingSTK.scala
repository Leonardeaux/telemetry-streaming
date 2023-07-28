package SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StructType, TimestampType}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.feature.VectorAssembler

case class LapInfos(timestamp: java.sql.Timestamp, laps: Int, speedKmh: Double)
case class LapAverageSpeed(sum: Double, count: Int)
case class LapInfosWithAverageSpeed(laps: Int, averageSpeed: Double)

object StructuredStreamingSTK {

    // Create DataFrame Schema
    private val base_schema = new StructType()
      .add("timestamp", TimestampType)
      .add("gas", DoubleType)
      .add("brake", DoubleType)
      .add("speedKmh", IntegerType)
      .add("laps", IntegerType)
      .add("abs", BooleanType)
      .add("steerAngle", DoubleType)
      .add("fuel", DoubleType)
      .add("maxFuel", DoubleType)
      .add("distance", IntegerType)

    def main(args: Array[String]): Unit = {
        // Set the log level to only print errors
        val rootLogger = Logger.getRootLogger
        rootLogger.setLevel(Level.WARN)

        // Create a SparkSession
        val spark: SparkSession = SparkSession.builder()
          .master("local[*]")
          .appName("Streaming")
          .getOrCreate()

        val query1 = SocketDataToKafka(spark)

        val query2 = KafkaDataToAveragePerLaps(spark)

//        val query3 = KafkaDataToSpeedPrediction(spark)    // Not Working

        val query4 = KafkaDataFuelConsoPerKm(spark)

//        val query5 = KafkaDataFuelConsoPerKmFor10Sec(spark)

        query1.awaitTermination()
        query2.awaitTermination()
//        query3.awaitTermination()
        query4.awaitTermination()
//        query5.awaitTermination()

        spark.stop()
    }

    /* ----------------- Write raw data to Kafka cluster ----------------- */
    private def SocketDataToKafka(sparkSession: SparkSession): StreamingQuery = {
        // Socket Server variables
        val serverName = "localhost"
        val port = "12346"

        // Read data from socket
        val base_df = sparkSession.readStream
          .format("socket")
          .option("host", serverName)
          .option("port", port)
          .load()

        // Convert data from JSON
        val jsonDF = base_df.select(from_json(col("value"), base_schema).as("data")).select("data.*")

        // Write data to Kafka
        // (topic0) => Base data
        val query1 = jsonDF.selectExpr("CAST(timestamp AS STRING) AS key", "to_json(struct(*)) AS value")
          .writeStream
          .format("kafka")
          .outputMode("append")
          .option("kafka.bootstrap.servers", "localhost:29092")
          .option("topic", "topic0")
          .option("checkpointLocation", "checkpoint")
          .start()

        query1
    }

    /* ----------------- Get Average Speed per Laps and Write to Kafka ----------------- */
    private def KafkaDataToAveragePerLaps(sparkSession: SparkSession): StreamingQuery = {
        import sparkSession.implicits._

        // Read data from Kafka
        // (topic0) => Base data
        val df_gas = sparkSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:29092")
          .option("subscribe", "topic0")
          .load()

        val df_str = df_gas.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        val jsonDF = df_str.select(from_json(col("value"), base_schema).as("data")).select("data.*")

        val lapInfosDS = jsonDF.as[LapInfos]

        def updateAverageSpeed(nb_lap: Int, values: Iterator[LapInfos], state: GroupState[LapAverageSpeed]): LapInfosWithAverageSpeed = {
            var newState = if (state.exists) state.get else LapAverageSpeed(0, 0)
            for (value <- values) {
                newState = LapAverageSpeed(newState.sum + value.speedKmh, newState.count + 1)
            }
            state.update(newState)
            LapInfosWithAverageSpeed(nb_lap, newState.sum / newState.count)
        }

        val speedAveragePerLap = lapInfosDS
          .groupByKey(_.laps)
          .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAverageSpeed)
          .as[LapInfosWithAverageSpeed]

        //        val query2 = speedAveragePerLap.writeStream
        //          .format("console")
        //          .outputMode("update")
        //          .start()
        val query2 = speedAveragePerLap.selectExpr("CAST(laps AS STRING) AS key", "to_json(struct(*)) AS value")
          .writeStream
          .format("kafka")
          .outputMode("update")
          .option("kafka.bootstrap.servers", "localhost:29092")
          .option("topic", "topic1")
          .option("checkpointLocation", "checkpoint1")
          .trigger(Trigger.ProcessingTime("15 seconds"))
          .start()

        query2
    }

    /* ----------------- Gas to Speed Transformation and Write Data to Kafka ----------------- */
    private def KafkaDataToSpeedPrediction(sparkSession: SparkSession): StreamingQuery = {
        // Read data from Kafka
        // (topic0) => Base data
        val df_gas = sparkSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:29092")
          .option("subscribe", "topic0")
          .load()

        val df_str = df_gas.selectExpr("CAST(value AS STRING)")

        val jsonDF = df_str
          .select(from_json(col("value").cast("string"), base_schema).as("data")).select("data.*")

        val assembler = new VectorAssembler().setInputCols(Array("speedKmh", "gas")).setOutputCol("features")

        val streamingFeatures = assembler.transform(jsonDF)

        val lr_model = LinearRegressionModel.load("models/lr_model_0")

        val streamingPrediction = lr_model.transform(streamingFeatures)

        val query3 = streamingPrediction.writeStream
          .format("console")
          .outputMode("append")
          .start()
//        val query2 = speedAveragePerLap.selectExpr("CAST(laps AS STRING) AS key", "to_json(struct(*)) AS value")
//          .writeStream
//          .format("kafka")
//          .outputMode("update")
//          .option("kafka.bootstrap.servers", "localhost:29092")
//          .option("topic", "topic2")
//          .option("checkpointLocation", "checkpoint1")
//          .start()

        query3
    }

    /* ----------------- Get Fuel Consomation ----------------- */
    private def KafkaDataFuelConsoPerKm(sparkSession: SparkSession): StreamingQuery = {
        import sparkSession.implicits._
        // Read data from Kafka
        // (topic0) => Base data
        val df_gas = sparkSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:29092")
          .option("subscribe", "topic0")
          .load()

        val df_str = df_gas.selectExpr("CAST(value AS STRING)")

        val jsonDF = df_str
          .select(from_json(col("value").cast("string"), base_schema).as("data")).select("data.*")

        // Consomation per kilometer = (maxFuel - fuel) / distance
        val consumptionPerKmDF = jsonDF
          .withColumn("consumptionPerKm", (col("maxFuel") - col("fuel")) / (col("distance") / 1000) * 100)

        val query4 = consumptionPerKmDF.selectExpr("CAST(timestamp AS STRING) AS key", "to_json(struct(*)) AS value")
          .writeStream
          .format("kafka")
          .outputMode("update")
          .option("kafka.bootstrap.servers", "localhost:29092")
          .option("topic", "topic3")
          .option("checkpointLocation", "checkpoint2")
          .trigger(Trigger.ProcessingTime("5 seconds"))
          .start()

        query4
    }

    /* ----------------- Get Fuel Consomation ----------------- */
    private def KafkaDataFuelConsoPerKmFor10Sec(sparkSession: SparkSession): StreamingQuery = {
        import sparkSession.implicits._
        // Read data from Kafka
        // (topic0) => Base data
        val df_gas = sparkSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:29092")
          .option("subscribe", "topic0")
          .load()

        val df_str = df_gas.selectExpr("CAST(value AS STRING)")

        val jsonDF = df_str
          .select(from_json(col("value").cast("string"), base_schema).as("data")).select("data.*")

        val consumptionPerKmDF = jsonDF
          .withWatermark("timestamp", "10 seconds")
          .groupBy(window($"timestamp", "10 seconds", "10 seconds"))
          .agg(
              (max($"fuel") - min($"fuel")) / ((max($"distance") / 1000) -  (min($"distance") / 1000)) * 100 as "consumptionPerKm"
          )
//
//        val query5 = consumptionPerKmDF.writeStream
//          .format("console")
//          .outputMode("complete")
//          .start()

        val query5 = consumptionPerKmDF.selectExpr("CAST(window AS STRING) AS key", "to_json(struct(*)) AS value")
          .writeStream
          .format("kafka")
          .outputMode("append")
          .option("kafka.bootstrap.servers", "localhost:29092")
          .option("topic", "topic4")
          .option("checkpointLocation", "checkpoint3")
          .start()

        query5
    }
}