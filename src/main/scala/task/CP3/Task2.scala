package task.CP3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Task2 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark: SparkSession = SparkSession
    .builder
    .appName("BigDataCP3.Task1")
    .master("local[*]")
    .getOrCreate()

  val validData: DataFrame = Common.extractValidData(spark)

  val featureAssembler: VectorAssembler = new VectorAssembler()
    .setInputCols(Array("Duration", "Time_Start"))
    .setOutputCol("features")

  val dataWithFeatures: DataFrame = featureAssembler.transform(validData)


  val scaler: StandardScaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithMean(true)
    .setWithStd(true)

  val scaledData: DataFrame = scaler.fit(dataWithFeatures).transform(dataWithFeatures)

  // Apply K-Means Clustering
  val kmeans: KMeans = new KMeans()
    .setK(3)
    .setFeaturesCol("scaledFeatures")
    .setPredictionCol("cluster")
    .setSeed(12345)

  val kmeansModel: KMeansModel = kmeans.fit(scaledData)

  // Add Cluster Labels to the Data
  val clusteredData: DataFrame = kmeansModel.transform(scaledData)

  println("Clustered Data:")
  clusteredData.select("User_ID", "Stream_ID", "Duration", "Time_Start", "cluster").show(false)

  // Analyze Cluster Centers
  println("Cluster Centers:")
  kmeansModel.clusterCenters.foreach(println)

  val clusters = clusteredData.select("cluster").distinct().collect().map(_.getInt(0))

  // Initialize a structure to store recommendations
  import scala.collection.mutable
  val allClusterRecommendations = mutable.Map[Int, (Common.Recommendations, DataFrame)]()

  // Step 3: Loop through clusters and generate recommendations
  clusters.foreach { clusterId =>
    println(s"Generating recommendations for Cluster $clusterId...")

    // Filter data for the current cluster
    val clusterData = clusteredData.filter(col("cluster") === clusterId)

    // Generate recommendations for the current cluster
    val recommendations = Common.findTopRecommendations(10, clusterData)

    val recsForNewUsers = Common.generateRecsForNewUsers(recommendations, clusterData)

    // Store recommendations
    allClusterRecommendations(clusterId) = (recommendations, recsForNewUsers)
  }

  // Display recommendations for each cluster
  allClusterRecommendations.foreach { case (clusterId, (recommendations, recs)) =>
    println(s"Recommendations for Cluster $clusterId:")
    recommendations.result.show()

    println(s"Recommendations for new users, cluster $clusterId:")
    recs.show()
  }

  spark.close()

}
