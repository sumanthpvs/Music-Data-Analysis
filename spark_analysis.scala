package spark_analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
//import org.apache.spark.streaming.streaming.flume._
import org.apache.spark.sql._
import org.apache.hadoop.hive._
object spark_analysis {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("Data Analysis Main_1")
      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
      .config("hive.metastore.uris","thrift://127.0.0.1:9083")
      .enableHiveSupport()
      .getOrCreate()

val batchId = args(0)
  val set_properties = sparkSession.sqlContext.sql("set hive.auto.convert.join=false")

val use_project_database = sparkSession.sqlContext.sql("USE project")
   //<<<<<<<<<---------- PROBLEM 5 - Creation of table and Insertion of data ------------>>>>>>>>>>>>
    //Determine top 10 unsubscribed users who listened to the songs for the longest duration.

    val create_hive_table_top_10_unsubscribed_users = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.top_10_unsubscribed_users"+
      "("+
      " user_id STRING,"+
      " song_id STRING,"+
      " artist_id STRING,"+
      " total_duration_in_minutes DOUBLE"+
      ")"+
      " PARTITIONED BY (batchid INT)"+
      " ROW FORMAT DELIMITED"+
      " FIELDS TERMINATED BY ','"+
      " STORED AS TEXTFILE")


    val insert_into_unsubscribed_users = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.top_10_unsubscribed_users"+
      s" PARTITION (batchid=$batchId)"+
      " SELECT"+
      " user_id,"+
      " song_id,"+
      " artist_id,"+
      " total_duration_in_minutes"+
      " FROM project.song_duration"+
      " WHERE user_type='unsubscribed'"+
      " AND total_duration_in_minutes>=0"+
      s" AND (batchid=$batchId)"+
      " ORDER BY total_duration_in_minutes desc"+
      " LIMIT 10")


  }
}

