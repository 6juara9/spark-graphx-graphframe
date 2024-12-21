package task.CP3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession // For DataFrame operations

object Task1 extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession
    .builder
    .appName("BigDataCP3.Task1")
    .master("local[*]")
    .getOrCreate()

  val validData = Common.extractValidData(spark)

  // Find top 10 recommendations for user

  val userRecommendations: Common.Recommendations = Common.findTopRecommendations(10, validData)

  println("Top 10 user recommendations: ")
  userRecommendations.result.show(false)

  // Find top 10 recommendations that were not in a train dataset

  val recommendationsForColdStartUsers = Common.generateRecsForNewUsers(userRecommendations, validData)

  recommendationsForColdStartUsers.show(false)

  spark.stop()
}
