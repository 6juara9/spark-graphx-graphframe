package task.CP3

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, explode, lit, row_number, struct}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Common {

  def extractValidData(spark: SparkSession): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("User_ID", IntegerType, nullable = true),
        StructField("Stream_ID", StringType, nullable = true), // Use StringType for Stream_ID
        StructField("Streamer_Username", StringType, nullable = true),
        StructField("Time_Start", IntegerType, nullable = true),
        StructField("Time_Stop", IntegerType, nullable = true)
      )
    )

    val rawData: DataFrame = spark.read
      .option("header", "false")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .csv("src/main/resources/StreamDataset2.csv")

    println(rawData.count())

    val enrichedData = rawData
      .withColumn("Duration", col("Time_Stop") - col("Time_Start")) // Calculate interaction duration
      .filter(col("Duration") > 0) // Keep only rows with positive duration

    // Handle nulls in critical columns
    enrichedData.na.drop(Seq("User_ID", "Stream_ID", "Streamer_Username", "Time_Start", "Time_Stop"))
  }

  final case class Recommendations(result: DataFrame, testDataset: DataFrame, traitDataset: DataFrame, bestTrainedModel: ALSModel)

  def findTopRecommendations(topNumber: Int, validData: DataFrame): Recommendations = {

    val streamMapping = validData
      .select("Stream_ID")
      .distinct()
      .withColumn("numeric_id", row_number().over(Window.orderBy(lit(1))) - 1) // Generate dense numeric IDs

    val enrichedDataWithIDs = validData.join(streamMapping, Seq("Stream_ID"))

    // Prepare data for ALS
    val ratings = enrichedDataWithIDs
      .select(
        col("User_ID").as("user"),
        col("numeric_id").cast(IntegerType).as("item"), // Dense numeric IDs for ALS
        col("Duration").as("rating")
      )

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2), seed = 12345)

    // Hyperparameters for tuning
    val ranks = Array(10, 20, 30)
    val regParams = Array(0.01, 0.1, 0.5)
    val maxIters = Array(3, 10, 20)

    var bestModel: ALS = null
    var bestRank = 0
    var bestRegParam = 0.0
    var bestMaxIter = 0
    var bestRmse = Double.MaxValue
    var bestTrainedModel = null.asInstanceOf[org.apache.spark.ml.recommendation.ALSModel]

    // Hyperparameter tuning
    for (rank <- ranks; regParam <- regParams; maxIter <- maxIters) {
      val als = new ALS()
        .setRank(rank)
        .setRegParam(regParam)
        .setMaxIter(maxIter)
        .setUserCol("user")
        .setItemCol("item")
        .setRatingCol("rating")
        .setColdStartStrategy("drop")

      val model = als.fit(training)

      // Evaluate model
      val predictions = model.transform(test)

      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")

      val rmse = evaluator.evaluate(predictions)
      println(s"Params: rank=$rank, regParam=$regParam, maxIter=$maxIter -> RMSE=$rmse")

      if (rmse < bestRmse) {
        bestModel = als
        bestRank = rank
        bestRegParam = regParam
        bestMaxIter = maxIter
        bestTrainedModel = model
        bestRmse = rmse
      }
    }

    // Print best model parameters
    println(s"Best Model Params: rank=$bestRank, regParam=$bestRegParam, maxIter=$bestMaxIter")
    println(s"Best RMSE: $bestRmse")

    // Final recommendations
    val userRecommendations = bestTrainedModel
      .recommendForAllUsers(topNumber)
      .withColumn("recommendations", explode(col("recommendations"))) // Explode recommendations array
      .select(
        col("user"),
        col("recommendations.item").as("numeric_id"), // Numeric ID assigned by ALS
        col("recommendations.rating") // Predicted rating
      )
      .join(streamMapping, Seq("numeric_id")) // Map numeric_id back to Stream_ID
      .groupBy("user") // Group back by user
      .agg(
        collect_list(struct(col("Stream_ID"), col("rating"))).as("recommendations") // Collect recommendations back into a nested structure
      )

    Recommendations(userRecommendations, test, training, bestTrainedModel)
  }

  def generateRecsForNewUsers(userRecommendations: Recommendations, validData: DataFrame) = {
    val trainUsers = userRecommendations.traitDataset.select("user").distinct()
    val testUsers = userRecommendations.testDataset.select("user").distinct()

    val validDataRenamed = validData.withColumnRenamed("User_ID", "user")
    val trainUsersRenamed = trainUsers.withColumnRenamed("User_ID", "user")
    val testUsersRenamed = testUsers.withColumnRenamed("User_ID", "user")

    // Визначаємо cold start користувачів
    val coldStartUsers = testUsersRenamed
      .join(trainUsersRenamed, testUsersRenamed("user") === trainUsersRenamed("user"), "left_anti")
      .select(testUsersRenamed("user"))

    println(s"Кількість cold start користувачів: ${coldStartUsers.count()}")

    // Знаходимо стріми, які переглядали cold start користувачі
    val coldStartStreams = validDataRenamed
      .join(coldStartUsers, validDataRenamed("user") === coldStartUsers("user"))
      .select("Stream_ID")
      .distinct()

    println(s"Кількість стрімів, переглянутих cold start користувачами: ${coldStartStreams.count()}")

    // Пошук схожих користувачів
    val similarUsers = validDataRenamed
      .join(trainUsersRenamed, validDataRenamed("user") === trainUsersRenamed("user")) // Тренувальні користувачі
      .join(coldStartStreams, validDataRenamed("Stream_ID") === coldStartStreams("Stream_ID")) // Спільні стріми
      .select(validDataRenamed("user").as("similar_user"))
      .distinct()

    println(s"Кількість схожих користувачів: ${similarUsers.count()}")

    // Призначення рекомендацій схожим користувачам
    val recommendationsForSimilarUsers = userRecommendations.bestTrainedModel
      .recommendForUserSubset(similarUsers.withColumnRenamed("similar_user", "user"), 10)

    println("Рекомендації для схожих користувачів:")
    recommendationsForSimilarUsers.show(false)

    // Призначення рекомендацій cold start користувачам
    val coldStartUsersAlias = coldStartUsers.as("cold_users")
    val recommendationsAlias = recommendationsForSimilarUsers.as("recs")


    coldStartUsersAlias.crossJoin(recommendationsAlias)
      .select(
        col("cold_users.user").as("cold_user"), // Новий користувач
        col("recs.recommendations.item").as("Stream_ID"), // Рекомендовані стріми
        col("recs.recommendations.rating") // Оцінка
      )
  }

}
