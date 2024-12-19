package task.CP2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

/**
  * a) знайти набір даних для створення рекомендацій в структурованому вигляді. Не
  * менше 100 тис. рядків (бажано 1 млн.),
  *
  * src/main/resources/3D_spatial_network.txt
  *
  * details here: https://archive.ics.uci.edu/dataset/246/3d+road+network+north+jutland+denmark
  *
  * Тривимірна мережа доріг із високоточною інформацією про висоту (+-20 см) із Данії,
  * яка використовується в алгоритмах маршрутизації екологічного маршруту та оцінки палива/Co2.
  *
  * format: OSM_ID,LONGITUDE,LATITUDE,ALTITUDE(mts)
  *
  * 1. OSM_ID: OpenStreetMap ID for each road segment or edge in the graph.
  * 2. LONGITUDE: Web Mercaptor (Google format) longitude
  * 3. LATITUDE: Web Mercaptor (Google format) latitude
  * 4. ALTITUDE: Height in meters.
  *
  *
  */

object Task1 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder
    .appName("BigDataCP2.Task2")
    .master("local[*]")
    .getOrCreate()

  val schema = StructType(
    Seq(
      StructField("OSM_ID", IntegerType, nullable = false),
      StructField("LONGITUDE", DoubleType, nullable = false),
      StructField("LATITUDE", DoubleType, nullable = false),
      StructField("ALTITUDE", DoubleType, nullable = false)
    )
  )

  val rawData: DataFrame = spark
    .read
    .option("header", "false")
    .option("inferSchema", "true")
    .csv("src/main/resources/3D_spatial_network.txt")

  val data = spark.createDataFrame(rawData.rdd, schema)

  println("Schema: ")
  data.printSchema()

  val labeledData = data.withColumn("label", when(col("ALTITUDE") > 25, 1).otherwise(0))

  println("LabeledData: ")
  labeledData.show(false)

  println("LabeledData separation: ")

  labeledData.groupBy("label").count().show()

  /**
   * Класифікація за логістичною регресією
   */

  // 2. Feature Engineering: Add interaction and polynomial terms
  val engineeredData = labeledData
    .withColumn("longitude_latitude", col("LONGITUDE") * col("LATITUDE")) // Interaction term
    .withColumn("longitude_squared", col("LONGITUDE") * col("LONGITUDE")) // Polynomial term
    .withColumn("latitude_squared", col("LATITUDE") * col("LATITUDE")) // Polynomial term

  val lrAssembler = new VectorAssembler()
    .setInputCols(Array("LONGITUDE", "LATITUDE", "longitude_latitude", "longitude_squared", "latitude_squared"))
    .setOutputCol("features")

  val assembledData = lrAssembler.transform(engineeredData).select("features", "label")

  // 4. Scale the features
  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithMean(true)
    .setWithStd(true)

  val scaledData = scaler.fit(assembledData).transform(assembledData).select("scaledFeatures", "label")

  // 5. Logistic Regression
  val logisticRegression = new LogisticRegression()
    .setFeaturesCol("scaledFeatures")
    .setLabelCol("label")

  val logisticRegressionParamGrid = new ParamGridBuilder()
    .addGrid(logisticRegression.regParam, Array(0.001, 0.01, 0.1, 0.2, 0.5, 0.7, 1))
    .build()

  /**
   * Класифікація за Random Forest
   */

  val rfAssembler = new VectorAssembler()
    .setInputCols(Array("LONGITUDE", "LATITUDE"))
    .setOutputCol("features")

  val dataset = rfAssembler.transform(labeledData).select("features", "label")

  val randomForest = new RandomForestClassifier()
    .setFeaturesCol("features")
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setSeed(123456)

  val randomForestParamGrid = new ParamGridBuilder()
    .addGrid(randomForest.numTrees, Array(2, 10, 20)) // Кількість дерев
    .addGrid(randomForest.maxDepth, Array(2, 5, 10)) // Максимальна глибина
    .addGrid(randomForest.featureSubsetStrategy, Array("sqrt", "log2", "all"))
    .build()

  /**
   * Виконати K-fold валідацію обраних методів класифікації; як seed значення для
   * генерації розбиття даних використати номер залікової книги; навести результати F1-
   * міри та accuracy, вивести ROC-криву.
   */

  def learnAndValidate(dataset: DataFrame, estimator: Estimator[_], estimatorParams: Array[ParamMap], metricName: String, method: String): DataFrame = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName(metricName)

    // Налаштування K-fold валідації
    val cv = new CrossValidator()
      .setEstimator(estimator)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(estimatorParams)
      .setNumFolds(10)
      .setSeed(123456)

    val cvModel = cv.fit(dataset)

    // Прогнозування
    val predictions = cvModel.transform(dataset)

    // Метрики точності
    val accuracy = evaluator.evaluate(predictions)
    println(s"Results of classification ($method - $metricName): $accuracy")
    println(s"Best parameters are ($method - $metricName): ${cvModel.bestModel.extractParamMap().toSeq.mkString("(", ", ", ")")}")
    predictions
  }

  def buildPOC(predictions: DataFrame, metric: String): Unit = {
    val predictionAndLabels = predictions
      .withColumn("prediction", col("prediction").cast("double"))
      .withColumn("label", col("label").cast("double"))
      .select("prediction", "label")
      .rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))

    // Ініціалізація метрик
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    // Отримання ROC-даних
    val roc = metrics.roc().collect()

    import breeze.linalg._
    import breeze.plot._

    val f = Figure()
    val p = f.subplot(0)

    // Конвертуємо точки ROC у два масиви
    val fpr = roc.map(_._1)
    val tpr = roc.map(_._2)

    // Побудова графіка
    p += plot(DenseVector(fpr), DenseVector(tpr), name = "ROC Curve")
    p.xlabel = "False Positive Rate"
    p.ylabel = "True Positive Rate"
    p.title = s"ROC Curve ($metric)"

    f.refresh()
  }

  val logisticRegressionPredictions = learnAndValidate(scaledData, logisticRegression, logisticRegressionParamGrid, "accuracy", "logisticRegression")
  learnAndValidate(scaledData, logisticRegression, logisticRegressionParamGrid, "f1", "logisticRegression")

  val randomForestPredictions = learnAndValidate(dataset, randomForest, randomForestParamGrid, "accuracy", "randomForest")
  learnAndValidate(dataset, randomForest, randomForestParamGrid, "f1", "randomForest")

  buildPOC(logisticRegressionPredictions, "logisticRegression")
  buildPOC(randomForestPredictions, "randomForest")

}
