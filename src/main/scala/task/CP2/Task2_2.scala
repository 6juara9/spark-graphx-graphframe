package task.CP2

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.sql.types._

import java.awt.Color
import breeze.linalg._
import breeze.plot._
import org.apache.log4j.{Level, Logger}

import java.awt.Color

/**
  * а) обрати не менше 2х методи кластеризації даних. Обґрунтувати свій вибір.
  * б) виконати підбір параметрів для виконання кластеризації. Провести аналіз
  * результатів підбору та оцінити результати експериментів.
  */

object Task2_2 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder
    .appName("BigDataCP2.Task2_1")
    .master("local[*]")
    .getOrCreate()
  // 2. Define schema and load data
  val schema = StructType(
    Seq(
      StructField("OSM_ID", IntegerType, nullable = false),
      StructField("LONGITUDE", DoubleType, nullable = false),
      StructField("LATITUDE", DoubleType, nullable = false),
      StructField("ALTITUDE", DoubleType, nullable = false)
    )
  )

  val rawData = spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .csv("src/main/resources/3D_spatial_network.txt")

  val data = spark.createDataFrame(rawData.rdd, schema)

  // 3. Feature transformation
  val assembler = new VectorAssembler()
    .setInputCols(Array("LONGITUDE", "LATITUDE", "ALTITUDE"))
    .setOutputCol("features")

  val dataset = assembler.transform(data)

  // 4. Hyperparameter tuning
  val kValues = Array(3, 5, 7, 10) // Number of clusters
  var bestLogLikelihood = Double.NegativeInfinity
  var bestK = 0
  var bestModel: GaussianMixture = null

  for (k <- kValues) {
    val gmm = new GaussianMixture()
      .setK(k)
      .setFeaturesCol("features")
      .setSeed(123456)

    val model = gmm.fit(dataset)

    // Log-likelihood as evaluation metric
    val logLikelihood = model.summary.logLikelihood
    println(s"Number of clusters (k=$k) -> Log-Likelihood: $logLikelihood")

    if (logLikelihood > bestLogLikelihood) {
      bestLogLikelihood = logLikelihood
      bestK = k
      bestModel = gmm
    }
  }

  println(s"Best Number of Clusters: $bestK")
  println(s"Best Log-Likelihood: $bestLogLikelihood")

  // 5. Train the final model with the best number of clusters
  val finalModel = bestModel.fit(dataset)
  val summary = finalModel.summary

  // Extract predictions (cluster assignments)
  val clusterAssignments = summary.predictions

  // Join the cluster assignments back to the original dataset
  val datasetWithClusters = dataset.join(
    clusterAssignments.select("features", "prediction"),
    Seq("features")
  ).withColumnRenamed("prediction", "cluster")

  println("Dataset with Clusters:")
  datasetWithClusters.show(false)

  // 7. Display cluster centers and covariance matrices
  println("Cluster Centers and Covariance Matrices:")
  finalModel.gaussians.zipWithIndex.foreach { case (gaussian, i) =>
    println(s"Cluster $i:")
    println(s"  Mean: ${gaussian.mean}")
    println(s"  Covariance: ${gaussian.cov}")
  }

  // 8. Visualization
  val clusterData = datasetWithClusters.select("LONGITUDE", "LATITUDE", "cluster").collect()
  val points = clusterData.map(row => (row.getDouble(0), row.getDouble(1), row.getInt(2)))

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
  p.title = s"GMM Clustering Visualization (k=$bestK)"
  f.refresh()

  // Stop Spark
  spark.stop()

}
