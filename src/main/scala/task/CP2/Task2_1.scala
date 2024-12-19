package task.CP2

import breeze.linalg.DenseVector
import breeze.plot.Figure
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.{ClusteringEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.awt.Color

/**
  * а) обрати не менше 2х методи кластеризації даних. Обґрунтувати свій вибір.
  * б) виконати підбір параметрів для виконання кластеризації. Провести аналіз
  * результатів підбору та оцінити результати експериментів.
  */

object Task2_1 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder
    .appName("BigDataCP2.Task2_1")
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

  // 3. Завантаження даних
  val rawData = spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .csv("src/main/resources/3D_spatial_network.txt")

  val data = spark.createDataFrame(rawData.rdd, schema)

  // 4. Формування ознак
  val assembler = new VectorAssembler()
    .setInputCols(Array("LONGITUDE", "LATITUDE", "ALTITUDE"))
    .setOutputCol("features")

  val dataset = assembler.transform(data)

  // 5. Гіперпараметри
  val kValues = Array(3, 5, 7, 10) // Кількість кластерів
  val maxIterValues = Array(10, 20, 50) // Максимальна кількість ітерацій
  val initModes = Array("k-means||", "random") // Методи ініціалізації

  var bestSilhouette = Double.NegativeInfinity
  var bestK = 0
  var bestMaxIter = 0
  var bestInitMode = ""

  // 6. Перебір параметрів
  for (k <- kValues; maxIter <- maxIterValues; initMode <- initModes) {
    val kmeans = new KMeans()
      .setK(k)
      .setFeaturesCol("features")
      .setMaxIter(maxIter)
      .setInitMode(initMode)
      .setSeed(123456)

    val model = kmeans.fit(dataset)

    // 7. Оцінка моделі
    val predictions = model.transform(dataset)

    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setMetricName("silhouette")

    val silhouette = evaluator.evaluate(predictions)
    println(s"Parameters: k=$k, maxIter=$maxIter, initMode=$initMode -> Silhouette: $silhouette")

    // 8. Зберігаємо найкращий результат
    if (silhouette > bestSilhouette) {
      bestSilhouette = silhouette
      bestK = k
      bestMaxIter = maxIter
      bestInitMode = initMode
    }
  }

  // Результати оптимізації
  println(s"Best Parameters: k=$bestK, maxIter=$bestMaxIter, initMode=$bestInitMode")
  println(s"Best Silhouette Score: $bestSilhouette")

  // Виконання кластеризації з найкращими параметрами
  val kmeans = new KMeans()
    .setK(bestK) // Set the number of clusters
    .setMaxIter(bestMaxIter)
    .setInitMode(bestInitMode)
    .setFeaturesCol("features")
    .setPredictionCol("cluster")
    .setSeed(123456)

  val model = kmeans.fit(dataset)

  val predictions = model.transform(dataset)

  println("Predictions:")
  predictions.show(false)

  val evaluator = new ClusteringEvaluator()
    .setFeaturesCol("features")
    .setPredictionCol("cluster")
    .setMetricName("silhouette") // Silhouette score

  val silhouette = evaluator.evaluate(predictions)
  println(s"Silhouette Score: $silhouette")

  println("Cluster Centers:")
  model.clusterCenters.foreach(center => println(center))

  val clusterData = predictions.select("LONGITUDE", "LATITUDE", "cluster").collect()

  import breeze.linalg._
  import breeze.plot._

  val points = clusterData.map(row => (row.getDouble(0), row.getDouble(1), row.getInt(2)))

  // Group points by cluster
  val pointsByCluster = points.groupBy(_._3)


  val f = Figure()
  val p = f.subplot(0)

  val colors = (0 until 10).map { i =>
    Color.getHSBColor(i / 10.0f, 0.9f, 0.9f)
  }.toArray

  pointsByCluster.foreach { case (cluster, clusterPoints) =>
    val x = DenseVector(clusterPoints.map(_._1).toArray) // LONGITUDE
    val y = DenseVector(clusterPoints.map(_._2).toArray) // LATITUDE
    val color = colors(cluster % colors.length)
    p += scatter(x, y, size = _ => 0.02d, _ => color)
  }

  p.xlabel = "Longitude"
  p.ylabel = "Latitude"
  p.title = "K-Means Clustering Visualization"

  f.refresh()
}
