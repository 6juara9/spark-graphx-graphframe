package task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import read.FilesReader

/**
 * Знайти найкоротший та найдовший маршрут між 2 заданими аеропортами.
 * За довжину рейсу беремо відстань між аеропортами. Необхідно врахувати маршрути з пересадками (не більше 4).
 */
object Task4 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder()
    .appName("kpi-big-data-task-4")
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

  // Selected airports
  val sourceAirportId = 1L
  val destAirportId = 5L

  // Функція для DFS пошуку
  def dfsFindPaths(current: Long, destination: Long, graph: Graph[String, Double], visited: List[Long], depth: Int): List[List[Long]] = {
    if (depth > 4) return Nil // Ліміт пересадок
    if (current == destination) return List(visited :+ current) // Дійшли до кінцевого аеропорту

    val neighbors = graph.edges.filter(_.srcId == current).map(_.dstId).collect()
    neighbors.flatMap { neighbor =>
      if (!visited.contains(neighbor)) {
        dfsFindPaths(neighbor, destination, graph, visited :+ current, depth + 1)
      } else {
        Nil
      }
    }.toList
  }

  val allPaths = dfsFindPaths(sourceAirportId, destAirportId, graph, List(), 0)

  // Calculate distances for each path
  val allPathsWithDistances = allPaths.map { path =>
    val distance = path
      .sliding(2)
      .collect { case Seq(src, dst) =>
        graph.edges.filter(e => e.srcId == src && e.dstId == dst).map(_.attr).collect().head
      }
      .sum
    (path, distance)
  }

  val shortestPath = allPathsWithDistances.minBy(_._2)
  println(s"Shortest path: ${shortestPath._1.mkString(" -> ")} with distance: ${shortestPath._2}")

  val longestPath = allPathsWithDistances.maxBy(_._2)
  println(s"Longest path: ${longestPath._1.mkString(" -> ")} with distance: ${longestPath._2}")

  spark.stop()

}
