package task.CP1

import _root_.util.Clock
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
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

  Clock.measure("Task5.ConnectedComponents") {

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

    // label propagation vs connected components

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

  }

  Clock.measure("Task5.LabelPropagation") {

    // Step 3: Run Label Propagation Algorithm (LPA)
    val verticesDf = airportsExtendedDf
      .select($"airport_id".cast("long").as("id"), $"airport_name")
      .distinct()


    val edgesDf = routesWithDistanceDf
      .select($"source_airport_id".cast("long").as("src"), $"dest_airport_id".cast("long").as("dst"), $"distance")
      .distinct()

    // Step 2: Create GraphFrame
    val graph1 = GraphFrame(verticesDf, edgesDf)

    // Step 3: Run Label Propagation Algorithm (LPA)
    val lpaResult = graph1.labelPropagation
      .maxIter(5) // Number of iterations
      .run()

    // Step 4: Extract clusters
    val clusters1 = lpaResult.groupBy("label").agg(collect_list("id").alias("airports"))

    // Step 5: Filter for large clusters
    val largeClusters1 = clusters1.filter(size($"airports") >= 5)

    // Step 6: Display results
    largeClusters1.show(false)

  }

  spark.stop()
}
