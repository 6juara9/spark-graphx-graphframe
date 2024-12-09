package task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import read.FilesReader
import util.Clock

/**
 * Знайти авіакомпанію з найбільшою сумою відстаней всіх рейсів. Теж саме з найменшою сумою.
 */
object Task1 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder()
    .appName("kpi-big-data-task-1")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val airportsExtendedDf = FilesReader.readExtendedAirports(spark)
  val routesWithDistanceDf = FilesReader.readRoutesWithDistances(spark, airportsExtendedDf)

  /**
    * Using GRAPHX
    */

  Clock.measure("Task1.GraphX") {

    // Prepare edges with airline and distance
    val edgesWithAirline: RDD[Edge[(String, Double)]] = routesWithDistanceDf
      .withColumn("source_airport_id", $"source_airport_id".cast("long"))
      .withColumn("dest_airport_id", $"dest_airport_id".cast("long"))
      .select($"source_airport_id", $"dest_airport_id", $"airline", $"distance")
      .rdd
      .map(row =>
        Edge(
          row.getAs[Long]("source_airport_id"),
          row.getAs[Long]("dest_airport_id"),
          (row.getAs[String]("airline"), row.getAs[Double]("distance"))
        )
      )

    // Aggregate distances by airline
    val airlineDistances = edgesWithAirline
      .map(edge => (edge.attr._1, edge.attr._2)) // (airline, distance)
      .reduceByKey(_ + _) // Sum distances for each airline

    // Task 1.1 Find the airline with the maximum total distance
    val maxAirline = airlineDistances.reduce((a, b) => {
      println(b); if (a._2 > b._2) a else b
    })
    println(s"Airline with max total distance [GraphX]: ${maxAirline._1}, distance: ${maxAirline._2}")

    // Task 1.2 Find the airline with the minimum total distance
    val minAirline = airlineDistances.reduce((a, b) => if (a._2 < b._2) a else b)
    println(s"Airline with min total distance [GraphX]: ${minAirline._1}, distance: ${minAirline._2}")
  }

  /**
    * Using GRAPH FRAME
    */

  Clock.measure("Task1.GraphFrame") {
    // Aggregate distances by airline
    val airlineDistancesDf = routesWithDistanceDf
      .groupBy($"airline")
      .agg(sum($"distance").alias("total_distance"))

    // Task 1.1 Find the airline with the maximum total distance
    val maxAirlineDf = airlineDistancesDf.orderBy($"total_distance".desc).first()
    println(s"Airline with max total distance [GraphFrame]: ${maxAirlineDf.getString(0)}, distance: ${maxAirlineDf.getDouble(1)}")

    // Task 1.2 Find the airline with the minimum total distance
    val minAirlineDf = airlineDistancesDf.orderBy($"total_distance".asc).first()
    println(s"Airline with min total distance [GraphFrame]: ${minAirlineDf.getString(0)}, distance: ${minAirlineDf.getDouble(1)}")

  }

  spark.stop()

}
