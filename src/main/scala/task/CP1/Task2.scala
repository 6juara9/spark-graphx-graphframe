package task.CP1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import read.FilesReader
import util.Clock

/**
  * Знайти всі можливі рейси між Польщею та Бельгією (при не більше ніж 2-х стиковках).
  * Використайте motif та інші варіанти рішень, порівняйте їх результати по швидкодії та складності реалізації.
  */
object Task2 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder()
    .appName("kpi-big-data-task-2")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val airportsExtendedDf = FilesReader.readExtendedAirports(spark)
  val routesWithDistanceDf = FilesReader.readRoutesWithDistances(spark, airportsExtendedDf)

  /**
    * USING GRAPH FRAME
    */

  Clock.measure("Task2.GraphFrame") {

    val vertices = airportsExtendedDf
      .select($"airport_id".alias("id"), $"country", $"airport_name")
      .distinct()

    val edges = routesWithDistanceDf
      .select($"source_airport_id".alias("src"), $"dest_airport_id".alias("dst"), $"distance", $"airline")

    // Create the GraphFrame
    val graphFrame = GraphFrame(vertices, edges)

    val motif = graphFrame
      .find("(a)-[e1]->(b); (b)-[e2]->(c)")
      .filter($"a.country" === "Poland" && $"c.country" === "Belgium")

    val directFlights = graphFrame
      .find("(a)-[e]->(c)")
      .filter($"a.country" === "Poland" && $"c.country" === "Belgium")

    motif.show(false)
    directFlights.show(false)

    println("Total flights from Poland to Belgium (with one airport in between) [DataFrame]: " + motif.count())
    println("Total direct flights from Poland to Belgium [DataFrame]: " + directFlights.count())

  }

  /**
    * USING GRAPHX
    */

  Clock.measure("Task2.GraphX") {
    val verticesRdd = airportsExtendedDf
      .select($"airport_id".cast("long"), $"country", $"airport_name")
      .rdd
      .map(row => (row.getAs[Long]("airport_id"), (row.getAs[String]("country"), row.getAs[String]("airport_name"))))

    // Create edges (routes)
    val edgesRdd = routesWithDistanceDf
      .select($"source_airport_id".cast("long"), $"dest_airport_id".cast("long"), $"distance")
      .rdd
      .map(row => Edge(row.getAs[Long]("source_airport_id"), row.getAs[Long]("dest_airport_id"), row.getAs[Double]("distance")))

    val graph = Graph(verticesRdd, edgesRdd)

    // Step 1: Find all triplets starting in Poland
    val firstLegs = graph
      .triplets
      .filter(triplet => triplet.srcAttr._1 == "Poland")
      .map(triplet => (triplet.dstId, (triplet.srcId, triplet.attr))) // (IntermediateAirportId, (SourceAirportId, Distance))

    // Step 2: Find all triplets ending in Belgium
    val secondLegs = graph
      .triplets
      .filter(triplet => triplet.dstAttr._1 == "Belgium")
      .map(triplet => (triplet.srcId, (triplet.dstId, triplet.attr))) // (IntermediateAirportId, (DestinationAirportId, Distance))

    // Step 3: Join first and second legs on IntermediateAirportId
    val joinedRoutes = firstLegs.join(secondLegs).map { case (intermediateAirportId, ((srcId, distance1), (dstId, distance2))) =>
      (srcId, intermediateAirportId, dstId, distance1 + distance2) // Combine legs into a full route
    }

    // Step 4: Find direct routes from Poland to Belgium
    val directRoutes = graph
      .triplets
      .filter(triplet => triplet.srcAttr._1 == "Poland" && triplet.dstAttr._1 == "Belgium")
      .map(triplet => (triplet.srcId, triplet.dstId, triplet.attr))

    // Print results
    println(s"Total direct flights from Poland to Belgium [GraphX]: ${directRoutes.count()}")
    println(s"Total intermediate routes (with 1 stop in between) [GraphX]: ${joinedRoutes.count()}")
  }

  spark.stop()

}
