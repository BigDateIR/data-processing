import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.glassfish.jersey.internal.jsr166.Flow.Subscriber
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.parsing.json._
import org.apache.spark.streaming.dstream.DStream

case class user_info(id: String, name: String)

case class coordinates(latitude: Double, longitude: Double)

case class TweetInfo(id: String, text: String, hashtags: Seq[String], timestamp: String, location: coordinates, sentiment: String, user: user_info)

object streaming_data_processing {
  def main(args: Array[String]): Unit = {

    //    configuration for kafka
    val brokers = "localhost:9092"
    val groupId = "GRP1"
    val topics = "test2" // multiple topics (separated by comma)

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
        val nestedDF = splitDF.map(r => TweetInfo(r.getString(0), r.getString(1), r.getAs[Seq[String]](2), r.getString(5), coordinates(r.getDouble(3), r.getDouble(4)), "sentiment", user_info(r.getString(6), r.getString(7))))
        //        nestedDF.show(false)

        // Convert DataFrame to JSON strings
        val jsonDataset = nestedDF.toJSON

        // Collect JSON strings to driver
        val jsonArray = jsonDataset.collect()

        // Combine into a single JSON array string
        val jsonString = jsonArray.mkString("[", ",", "]")

        // Output the JSON string
//        println(jsonString)
      }
    }

    //**
    ssc.start()
    ssc.awaitTermination()
    //
  }
}
