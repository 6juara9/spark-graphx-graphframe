package read

import org.apache.spark.sql.functions.{acos, cos, radians, sin}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FilesReader {

  def readExtendedAirports(session: SparkSession): DataFrame =
    session
      .read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("nullValue", "\\N")
      .csv("src/main/resources/airports-extended.dat")
      .toDF(
        "airport_id",
        "airport_name",
        "city",
        "country",
        "iata",
        "icao",
        "latitude",
        "longitude",
        "altitude",
        "timezone",
        "dst",
        "tz_timezone",
        "airport_type",
        "source"
      )

  def readRoutesWithDistances(session: SparkSession, airportsDataFrame: DataFrame): DataFrame = {
    import session.implicits._

    session
      .read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("nullValue", "\\N")
      .csv("src/main/resources/routes.dat")
      .toDF(
        "airline",
        "airline_id",
        "source_airport",
        "source_airport_id",
        "dest_airport",
        "dest_airport_id",
        "codeshare",
        "stops",
        "equipment"
      )
      .join(airportsDataFrame.alias("src_airport"), $"source_airport_id" === $"src_airport.airport_id")
      .join(airportsDataFrame.alias("dst_airport"), $"dest_airport_id" === $"dst_airport.airport_id")
      .select(
        $"source_airport_id",
        $"dest_airport_id",
        $"src_airport.latitude".alias("src_lat"),
        $"src_airport.longitude".alias("src_lon"),
        $"dst_airport.latitude".alias("dst_lat"),
        $"dst_airport.longitude".alias("dst_lon"),
        $"airline",
        $"stops",
        acos(
          sin(radians($"src_lat")) * sin(radians($"dst_lat")) +
          cos(radians($"src_lat")) * cos(radians($"dst_lat")) *
          cos(radians($"dst_lon") - radians($"src_lon"))
        ) * 6371 as "distance"
      )
  }

}
