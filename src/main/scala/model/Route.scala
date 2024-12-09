package model

import scala.util.Try

final case class Route(
  airlineCode: String, // Код авіакомпанії
  airlineId: Option[Long], // ID авіакомпанії (може бути відсутнім)
  sourceAirportCode: String, // Код IATA аеропорту відправлення
  sourceAirportId: Long, // ID аеропорту відправлення
  destAirportCode: String, // Код IATA аеропорту прибуття
  destAirportId: Long, // ID аеропорту прибуття
  codeshare: Option[String], // Код спільного використання (може бути відсутнім)
  stops: Int, // Кількість зупинок
  equipment: Option[String] // Тип літака (може бути відсутнім)
)

object Route {

  def fromString(line: String): Option[Route] = Try {
    val fields = line.split(",")
    Route(
      airlineCode = fields(0),
      airlineId = Option.when(fields(1) != "\\N")(fields(1).toLong),
      sourceAirportCode = fields(2),
      sourceAirportId = fields(3).toLong,
      destAirportCode = fields(4),
      destAirportId = fields(5).toLong,
      codeshare = Option.when(fields(6) != "\\N")(fields(6)),
      stops = fields(7).toInt,
      equipment = Option.when(fields.isDefinedAt(8))(fields(8))
    )
  }.toOption

  def unsafe(line: String): Route = fromString(line)
    .getOrElse(throw new RuntimeException("Can't parse line into Route model"))

}
