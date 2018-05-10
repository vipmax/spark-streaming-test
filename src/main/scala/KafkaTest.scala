import org.apache.spark.sql.SparkSession

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("KafkaTest")
      .getOrCreate()

    import spark.implicits._

    val input = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "test2")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val query = input
      .flatMap(_._2.split(" "))
      .groupBy("value")
      .count()
      .sort($"count")

    query.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }
}
