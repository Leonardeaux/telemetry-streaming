package SparkCore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object SVMOnABS {
    def main(args: Array[String]): Unit = {
        // Set the log level to only print errors
        val rootLogger = Logger.getRootLogger
        rootLogger.setLevel(Level.WARN)

        val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("SVM Model")
          .getOrCreate()

        val data = spark
          .read
          .option("inferSchema", "true")
          .format("json")
          .load("data/telemetry2.json")

        // Convertir les colonnes de caractéristiques en un seul vecteur
        val assembler = new VectorAssembler()
          .setInputCols(Array("speed", "brake", "steerAngle"))
          .setOutputCol("features")

        val features = assembler.transform(data)

        val Array(trainingData, testData) = features.randomSplit(Array(0.7, 0.3))

        val lsvc = new LinearSVC()
          .setMaxIter(10)
          .setRegParam(0.1)
          .setLabelCol("abs")
          .setFeaturesCol("features")

        val SVM_Model = lsvc.fit(trainingData)

        val predictions = SVM_Model.transform(testData)
        predictions.select("prediction", "abs", "features").show(5)

        SVM_Model.save("models/svm_model_0")

        spark.stop()
    }
}
