import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.functions._


import java.io.File
import scala.collection.mutable

import org.apache.poi.xssf.usermodel.XSSFWorkbook

case class user_info(id: String, name: String)

case class coordinates(latitude: Double, longitude: Double)

case class TweetInfo(id: String, text: String, hashtags: Seq[String], timestamp: String, location: coordinates, sentiment: String, user: user_info)

object streaming_data_processing {
  def main(args: Array[String]): Unit = {
    val filePath = "C:\\Users\\Admin\\Desktop\\pro_big_scala\\Copy of Positive and Negative Word List.xlsx"

    val workbook = new XSSFWorkbook(new File(filePath))

    val sheet = workbook.getSheetAt(0)
    val badWordsSet = mutable.Set[String]()
    val goodWordsSet = mutable.Set[String]()


    for (row <- 1 to sheet.getPhysicalNumberOfRows - 1) {
      val badWordCell = sheet.getRow(row).getCell(0)
      val goodWordCell = sheet.getRow(row).getCell(1)

      if (badWordCell != null) {
        badWordsSet.add(badWordCell.getStringCellValue)
      }
      if (goodWordCell != null) {
        goodWordsSet.add(goodWordCell.getStringCellValue)
      }
    }



    //    configuration for kafka
    val brokers = "localhost:9092"
    val groupId = "GRP1"
    val topics = "my-topic" // multiple topics (separated by comma)

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming_data_processing")
    val ssc = new StreamingContext(SparkConf, Seconds(10))
    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")
    // To avoid logging too many debugging messages
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    import spark.implicits._

    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )

    // Extract JSON values from Kafka messages
    val jsonStream = messages.map(record => record.value())
    val sentimentUDF = udf((text: String) => {
      val words = text.toLowerCase.split("\\W+")
      val goodScore = words.count(word => goodWordsSet.contains(word))
      val badScore = words.count(word => badWordsSet.contains(word))

      if (goodScore > badScore) "Positive"
      else if (badScore > goodScore) "Negative"
      else "Neutral"
    })
    // Process each RDD in the DStream
    jsonStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Convert RDD[String] to DataFrame
        val df = spark.read.json(rdd)

        //        df.show(truncate = false)
        val wanted_data = df.select("tweet_id", "text", "hashtags", "lat", "lon", "created_at", "user_id", "user_name")
        //        wanted_data.show(truncate = false)
        val cleaned_df = wanted_data.na.fill("N/A", Seq("tweet_id", "text", "hashtags", "created_at", "user_id", "user_name")).na.fill(0.0f, Seq("lat", "lon"))
        //        cleaned_df.show(truncate = false)
        val splitDF = cleaned_df.withColumn("hashtags", split(col("hashtags"), ","))
        //        splitDF.show(false)
         val secall =splitDF.withColumn("sentiment", sentimentUDF(col("text")))


        val nestedDF = secall.map(r => TweetInfo(r.getString(0), r.getString(1), r.getAs[Seq[String]](2), r.getString(5), coordinates(r.getDouble(3), r.getDouble(4)),r.getString(8), user_info(r.getString(6), r.getString(7))))
        //        nestedDF.show(false)

        // Convert DataFrame to JSON strings
        val jsonDataset = nestedDF.toJSON

        // Collect JSON strings to driver
        val jsonArray = jsonDataset.collect()

        // Combine into a single JSON array string
        val jsonString = jsonArray.mkString("[", ",", "]")

        // Output the JSON string
              println(jsonString)

      }
    }

    //**
    ssc.start()
    ssc.awaitTermination()
    //
  }
}