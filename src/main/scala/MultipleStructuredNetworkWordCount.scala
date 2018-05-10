import org.apache.spark.sql.SparkSession

object MultipleStructuredNetworkWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("MultipleStructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    val sourcesInfo = List(
      ("localhost", 9999),
      ("localhost", 9998)
    )

    // Create sources
    val sources = sourcesInfo.map{ case (host, port) => spark.readStream
      .format("socket").option("host", host).option("port", port).load()
    }

    // Union sources
    val lines = sources.reduce(_ union _)

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }
}