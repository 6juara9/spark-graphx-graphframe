package task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import read.FilesReader

/**
 * Знайти підграфи, з кількістю вузлів не менше 2 зв’язаних між собою, які не мають зв’язків з іншими аеропортами.
 */
object Task6 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder()
    .appName("kpi-big-data-task-6")
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

  val largeClusters = clusters.filter { case (_, vertices) => vertices.size >= 2 }

  // Check for isolation: does the component have connections to other components?
  val isolatedClusters = largeClusters.filter { case (_, vertices) =>
    val nodeSet = vertices.toSet
    val externalEdges = graph
      .edges
      .filter(edge =>
        (nodeSet.contains(edge.srcId) && !nodeSet.contains(edge.dstId)) ||
        (nodeSet.contains(edge.dstId) && !nodeSet.contains(edge.srcId))
      )
      .count()

    externalEdges == 0 // Isolated, if there are no external connections / edges
  }

  isolatedClusters.collect().foreach { case (componentId, vertices) =>
    println(s"Isolated Subgraph Component ID: $componentId, Airports: ${vertices.mkString(", ")}")
  }

  spark.stop()
}
