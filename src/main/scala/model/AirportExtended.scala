package model

import scala.util.Try

final case class AirportExtended(
  id: Long, // ID аеропорту/станції
  name: String, // Назва
  city: String, // Місто
  country: String, // Країна
  latitude: Double, // Широта
  longitude: Double, // Довгота
  altitude: Double, // Висота над рівнем моря
  timezone: Option[Int], // Часовий пояс (може бути відсутнім)
  dst: Option[String] // Літній час (E, N, A тощо, може бути відсутнім)
)

object AirportExtended {

  def fromString(line: String): Option[AirportExtended] = Try {
    val fields = line.split(",")
    AirportExtended(
      id = fields(0).toLong,
      name = fields(1).replaceAll("\"", ""),
      city = fields(2).replaceAll("\"", ""),
      country = fields(3).replaceAll("\"", ""),
      latitude = fields(6).toDouble,
      longitude = fields(7).toDouble,
      altitude = fields(8).toDouble,
      timezone = Option.when(fields(9) != "\\N")(fields(9).toInt),
      dst = Option.when(fields(10) != "\\N")(fields(10))
    )
  }.toOption

  def unsafe(line: String): AirportExtended = fromString(line)
    .getOrElse(throw new RuntimeException("Can't parse line into AirportExtended model"))

}
