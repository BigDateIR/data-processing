import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.glassfish.jersey.internal.jsr166.Flow.Subscriber

object streaming_data_processing {
  def main(args: Array[String]): Unit = {
//    configuration for kafka
    val brokers = "localhost:9092"
    val groupId = "GRP1"
    val topics = "TEST" // multiple topics (separated by comma)

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming_data_processing")
    val ssc = new StreamingContext(SparkConf, Seconds(3))
    val sc =ssc.sparkContext
    sc.setLogLevel("OFF")

    val topicSet=topics.split(",").toSet
    val kafkaParams=Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->brokers,
      ConsumerConfig.GROUP_ID_CONFIG->groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer]
    )

    val messages=KafkaUtils.createDirectStream[String,String](
      ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topicSet,kafkaParams)
    )

//    data processing
    val line = messages.map(_.value)
    val words=line.flatMap(_.split(" "))
    val wordCounts=words.map(x=>(x,1L)).reduceByKey(_+_)
    wordCounts.print()
//**
    ssc.start()
    ssc.awaitTermination()

  }
}
