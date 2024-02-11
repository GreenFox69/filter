import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object filter {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Sergei_Anisimov_lab04a")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    import spark.implicits._

      val offset: String = spark.sparkContext.getConf.get("spark.filter.offset","earliest")
      val topicName: String = spark.sparkContext.getConf.get("spark.filter.topic_name","lab04_input_data")

      val topic:String =  if (topicName.isEmpty) "lab04_input_data" else topicName

      val output_dir_prefix: String =  spark.sparkContext.getConf.get("spark.filter.output_dir_prefix","/user/sergei.anisimov/visits")

      var output_dir = "/user/sergei.anisimov/visits"

      if (output_dir_prefix.startsWith("file://")) output_dir = output_dir_prefix + "/"
      if (output_dir_prefix.startsWith("/user/sergei.anisimov")) output_dir = output_dir_prefix





    val kafkaDF = spark.read.format("kafka")
      .option("kafka.bootstrap.servers","spark-master-1:6667")
      .option("subscribe", topic )
      .option("startingOffsets",
        if(offset.contains("earliest"))
          offset
        else {
          "{\"" + topic + "\":{\"0\":" + offset + "}}"
        }
      )
      .load()


    val jsonDF = kafkaDF.select($"value".cast("string")).as[String]

    val visitsDF = spark.read.json(jsonDF)
      .withColumn("date", from_unixtime(col("timestamp")/1000, "yyyyMMdd"))
      .select($"event_type",$"category",$"item_id",$"item_price"
        ,$"timestamp",$"uid",$"date",$"date".as("p_date"))

    val viewDF = visitsDF.filter($"event_type" === "view")
    val buyDF = visitsDF.filter($"event_type" === "buy")

    viewDF.write
      .format("json")
      .partitionBy("p_date")
      .mode("overwrite")
      .save(output_dir+"/view")

    buyDF.write
      .format("json")
      .partitionBy("p_date")
      .mode("overwrite")
      .save(output_dir+"/buy")


  }

}
