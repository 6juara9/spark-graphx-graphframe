ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"
resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/" // to download apache graph frame

lazy val root = (project in file("."))
  .settings(
    name := "spark-graphx",
    libraryDependencies ++= Seq(

      "org.apache.spark" %% "spark-graphx" % "4.0.0-preview2" exclude("com.esotericsoftware", "kryo"),
      "org.apache.spark" %% "spark-sql" % "4.0.0-preview2",
      "com.esotericsoftware" % "kryo" % "4.0.2",
      "graphframes" % "graphframes" % "0.8.4-spark3.5-s_2.13"
    )
  )
