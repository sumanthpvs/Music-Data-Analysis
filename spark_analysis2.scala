package spark_analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
//import org.apache.spark.streaming.streaming.flume._
import org.apache.spark.sql._
import org.apache.hadoop.hive._

object Spark_analysis_2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local").appName("Spark Session example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport().getOrCreate()
    val batchId = args(0)

    //sparkSession.sqlContext.sql("USE project")
    //sparkSession.sqlContext.sql("SELECT station_id from top_10_stations").show()
    //sparkSession.sqlContext.sql("SELECT user_type,total_duration_in_minutes from song_duration").show()
    //sparkSession.sqlContext.sql("SELECT artist_id from connected_artists").show()
    //sparkSession.sqlContext.sql("SELECT song_id from top_10_songs_maxrevenue").show()
    sparkSession.sqlContext.sql("SELECT user_id from top_10_unsubscribed_users").show()
  }
}
