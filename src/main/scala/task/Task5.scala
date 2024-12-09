package task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import read.FilesReader

/**
 * Виділити великі кластери аеропортів (не менше 5, на основі інформації про рейси).
 * Проаналізувати результати створення цих кластерів.
 */
object Task5 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder()
    .appName("kpi-big-data-task-5")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val airportsExtendedDf = FilesReader.readExtendedAirports(spark)
  val routesWithDistanceDf = FilesReader.readRoutesWithDistances(spark, airportsExtendedDf)

  val vertices: RDD[(Long, String)] = airportsExtendedDf
    .select($"airport_id".cast("long"), $"airport_name")
    .rdd
    .map(row => (row.getAs[Long]("airport_id"), row.getAs[String]("airport_name")))

  val edges: RDD[Edge[Double]] = routesWithDistanceDf
    .select($"source_airport_id".cast("long"), $"dest_airport_id".cast("long"), $"distance")
    .rdd
    .map(row => Edge(row.getAs[Long]("source_airport_id"), row.getAs[Long]("dest_airport_id"), row.getAs[Double]("distance")))

  val graph = Graph(vertices, edges)

  // Find connected components
  val connectedComponents = graph.connectedComponents().vertices

  // Grouping vertices by components
  val clusters = connectedComponents.map { case (vertexId, componentId) => (componentId, vertexId) }
    .groupByKey()

  // Filtering to use only big clusters
  val largeClusters = clusters.filter { case (_, vertices) => vertices.size >= 5 }

  largeClusters.collect().foreach { case (clusterId, vertices) =>
    println(s"Cluster ID: $clusterId, Airports: ${vertices.mkString(", ")}")
  }

  // Number of clusters: analysis
  val clusterSizes = largeClusters.map { case (clusterId, vertices) => (clusterId, vertices.size) }
  println(s"Number of large clusters: ${clusterSizes.count()}")
  clusterSizes.collect().foreach { case (clusterId, size) =>
    println(s"Cluster ID: $clusterId has $size airports")
  }

  spark.stop()
}
