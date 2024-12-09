package task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import read.FilesReader
import util.Clock

/**
 * Знайти аеропорти з найменшою та найбільшою кількістю рейсів.
 */
object Task3 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder()
    .appName("kpi-big-data-task-3")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val airportsExtendedDf = FilesReader.readExtendedAirports(spark)
  val routesWithDistanceDf = FilesReader.readRoutesWithDistances(spark, airportsExtendedDf)

  // Simple DataFrame API, pure Spark

  Clock.measure("Task3.DataFrame") {

    // Кількість рейсів для аеропортів відправлення
    val sourceCounts = routesWithDistanceDf
      .groupBy($"source_airport_id")
      .agg(count("*").alias("flight_count"))

    // Кількість рейсів для аеропортів прибуття
    val destCounts = routesWithDistanceDf
      .groupBy($"dest_airport_id")
      .agg(count("*").alias("flight_count"))

    // Загальна кількість рейсів (відправлення + прибуття)
    val totalCounts = sourceCounts
      .withColumnRenamed("source_airport_id", "airport_id")
      .union(destCounts.withColumnRenamed("dest_airport_id", "airport_id"))
      .groupBy($"airport_id")
      .agg(sum($"flight_count").alias("total_flights"))

    // Знайти аеропорт із найбільшою кількістю рейсів
    val maxFlights = totalCounts.orderBy($"total_flights".desc).limit(1)
    maxFlights.show(false)

    // Знайти аеропорт із найменшою кількістю рейсів
    val minFlights = totalCounts.orderBy($"total_flights".asc).limit(1)
    minFlights.show(false)
  }

  Clock.measure("Task3.GraphX") {
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

    val totalDegrees = graph.degrees // total number of edges

    // Find the airport with the maximum number of flights
    val maxFlights = totalDegrees.reduce((a, b) => if (a._2 > b._2) a else b)
    println(s"Airport with max flights: ${maxFlights._1}, total flights: ${maxFlights._2}")

    // Find the airport with the minimum number of flights
    val minFlights = totalDegrees.reduce((a, b) => if (a._2 < b._2) a else b)
    println(s"Airport with min flights: ${minFlights._1}, total flights: ${minFlights._2}")

  }

  spark.close()

}
