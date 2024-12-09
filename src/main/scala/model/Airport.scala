package model

import scala.util.Try

final case class Airport(
  id: Long, // ID аеропорту
  name: String, // Назва аеропорту
  city: String, // Місто
  country: String, // Країна
  iata: Option[String], // Код IATA (може бути відсутнім)
  icao: Option[String], // Код ICAO (може бути відсутнім)
  latitude: Double, // Широта
  longitude: Double, // Довгота
  altitude: Double // Висота над рівнем моря
)

object Airport {

  def fromString(line: String): Option[Airport] = Try {
    val fields = line.split(",")
    Airport(
      id = fields(0).toLong,
      name = fields(1).replaceAll("\"", ""),
      city = fields(2).replaceAll("\"", ""),
      country = fields(3).replaceAll("\"", ""),
      iata = Option.when(fields(4) != "\\N")(fields(4).replaceAll("\"", "")),
      icao = Option.when(fields(5) != "\\N")(fields(5).replaceAll("\"", "")),
      latitude = fields(6).toDouble,
      longitude = fields(7).toDouble,
      altitude = fields(8).toDouble
    )
  }.toOption

  def unsafe(line: String): Airport = fromString(line)
    .getOrElse(throw new RuntimeException("Can't parse line into Airport model"))

}
