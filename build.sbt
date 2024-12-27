ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "tweets processing",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.3",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.3",
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.8.1",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.3",
    dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.6-4"
  )
