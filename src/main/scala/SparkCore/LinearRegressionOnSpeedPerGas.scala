package SparkCore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object LinearRegressionOnSpeedPerGas {
    def main(args: Array[String]): Unit = {
        // Set the log level to only print errors
        val rootLogger = Logger.getRootLogger
        rootLogger.setLevel(Level.WARN)

        val spark: SparkSession = SparkSession.builder()
          .master("local[*]")
          .appName("Linear Regression Model")
          .getOrCreate()

        val data = spark
          .read
          .option("inferSchema","true")
          .format("json")
          .load("data/telemetry.json")

        val assembler = new VectorAssembler().setInputCols(Array("speed", "acceleration")).setOutputCol("features")

        val features = assembler.transform(data)

        val Array(trainingData, testData) = features.randomSplit(Array(0.7, 0.3))

        val lr = new LinearRegression()
          .setMaxIter(10)
          .setRegParam(0.3)
          .setElasticNetParam(0.8)
          .setLabelCol("speedT1")
          .setFeaturesCol("features")

        val lrModel = lr.fit(trainingData)

        val predictions = lrModel.transform(testData)
        predictions.show()

        lrModel.save("models/lr_model_0")

        spark.stop()
    }
}
