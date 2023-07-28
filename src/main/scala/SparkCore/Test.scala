package SparkCore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.classification.LinearSVCModel
import org.apache.spark.sql.SparkSession

object Test {
    def main(args: Array[String]): Unit = {
        // Set the log level to only print errors
        val rootLogger = Logger.getRootLogger
        rootLogger.setLevel(Level.WARN)

        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Linear Regression Model").getOrCreate()

        val data = spark.read.option("inferSchema", "true").format("json").load("C:/Users/enzol/IdeaProjects/scala-spark-maven/data/test.json")

        val assembler = new VectorAssembler().setInputCols(Array("speed", "brake", "steerAngle")).setOutputCol("features")

        val features = assembler.transform(data)

        val svm = LinearSVCModel.load("models/svm_model_0")

        val predictions = svm.transform(features)

        predictions.show()
    }
}
