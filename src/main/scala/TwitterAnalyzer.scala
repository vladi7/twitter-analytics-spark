//http://blog.madhukaraphatak.com/introduction-to-spark-two-part-2
//reference for word count done efficiently
package analyzer

import org.apache.spark.sql._
import java.util.regex.Pattern

import spray.json.DefaultJsonProtocol
import spray.json.DeserializationException
import spray.json.JsArray
import spray.json.JsNumber
import spray.json.JsObject
import spray.json.JsString
import spray.json.JsValue
import spray.json.JsonFormat
import spray.json.pimpAny
import spray.json.{DefaultJsonProtocol, JsonFormat}
import play.api.libs.json._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import com.google.gson.Gson
import com.google.gson.Gson
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.twitter._
import twitter4j.{Place, TwitterFactory, User}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import sys.process._
import scala.util.matching.Regex
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.functions.{col, concat_ws}
import java.util.{Calendar, Date}

import org.joda.time.{DateTime, DateTimeZone}

case class Tweet(id: Long,
                 text: String,
                 hashTags: String,
                 urls: String,
                 likes: Long)

object TwitterAnalyzer {
  def main(args: Array[String]) {

    def callPython(): Unit = {


      val consumerKey = args(0)replaceAll("\\s", "")
      val consumerSecret = args(1)replaceAll("\\s", "")
      val accessToken = args(3)replaceAll("\\s", "")
      val accessSecret = args(2)replaceAll("\\s", "")
      val languages = args(4)replaceAll("\\s", "")
      val words = args(5)replaceAll("\\s", "")
      val numberOfTweets = args(6)replaceAll("\\s", "")



      val result = "python TweetStreamer.py "+ consumerKey + " "+consumerSecret+" "+ accessToken+" "+accessSecret+" "+languages +" "+words+" "+numberOfTweets ! ProcessLogger(stdout append _, stderr append _)
      println(result)
      println(result.toString())
      val ok = result.toString().contains( "1")
      if(ok){
        System.exit(0)
      }
      print("+++++++++++++++++")
      println("stdout: " + stdout)
      println("stderr: " + stderr)
    }


    callPython()


    val ss = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    import ss.implicits._


    val data = ss.read.text("hdfs://localhost:9000/tweetsnew1.json").as[String]
    data.printSchema()
    data.createOrReplaceTempView("tweets")
    val text = ss.sql("SELECT * FROM tweets")
    val tweets = ss.read.json(ss.sql("SELECT * FROM tweets").as[String])
    tweets.createOrReplaceTempView("tweetswithschema")




    if(args(7).equalsIgnoreCase("Word Count Hashtags")||args(7).equalsIgnoreCase("Both")) {
      //Get hashtags and work on data to just have a table of hashtags
      val dataset = tweets.select($"id", $"text", $"entities.hashtags", $"entities.urls", $"favorite_count" as "likes").as[Tweet] //(Encoders.product[SimpleTuple])
      dataset.show(10)
      val hashtags = ss.sql("SELECT entities.hashtags.text FROM tweetswithschema")
      hashtags.filter($"text".isNotNull).show()
      hashtags.createOrReplaceTempView("hashtagtext")

      val newHashtags = hashtags.withColumn("newtext", concat_ws(" ", $"text"))
      val newFilteredHashtags = newHashtags.filter(!($"newtext" === ""))
      newFilteredHashtags.createOrReplaceTempView("hashtagsfiltered")

      val hashtagsFinal = ss.sql("SELECT newtext FROM hashtagsfiltered")


      //Run a word count on hashtags and urls

      // val pattern1 = new Regex("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)")

      val pattern1 = new Regex("^\\s*[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*\\s*$")

      //text.rdd.take(10).foreach(println)
      val countsmapped = hashtagsFinal.flatMap(sqlRow => (pattern1 findAllIn sqlRow(0).toString).toList)
      val countsreduced = countsmapped.groupByKey(_.toLowerCase)
      val counts = countsreduced.count().orderBy($"count(1)".desc)
      counts.show()


      import java.text.SimpleDateFormat
      import java.util.TimeZone
      val date = new Date
      val df = new SimpleDateFormat("yyyyMMddHHmmss")
      df.setTimeZone(TimeZone.getTimeZone("America/Chicago"))
      counts.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/Hashtags/Hashtags")
        //.save("Output/Hashtags/"+df.format(date)+"Hashtags")

    }
    //Get urls and work on data to just have a table of urls
    if(args(7).equalsIgnoreCase("Word Count URLs")||args(7).equalsIgnoreCase("Both")) {

      val urls = ss.sql("SELECT entities.urls.expanded_url FROM tweetswithschema")
      urls.filter($"expanded_url".isNotNull).show()

      val newUrls = urls.withColumn("newtext", concat_ws(" ", $"expanded_url"))
      val newFilteredUrls = newUrls.filter(!($"newtext" === ""))
      newFilteredUrls.createOrReplaceTempView("urlstagsfiltered")


      val urlssFinal = ss.sql("SELECT newtext FROM urlstagsfiltered")




      val pattern2 = new Regex("(www|http)\\S+")

      val countsmapped2 = urlssFinal.flatMap(sqlRow => (pattern2 findAllIn sqlRow(0).toString).toList)
      val countsreduced2 = countsmapped2.groupByKey(_.toLowerCase)
      val counts2 = countsreduced2.count().orderBy($"count(1)".desc)

      counts2.show()

      import java.text.SimpleDateFormat
      import java.util.TimeZone
      val date = new Date
      val df = new SimpleDateFormat("yyyyMMddHHmmss")
      df.setTimeZone(TimeZone.getTimeZone("America/Chicago"))
     // counts2.write.csv("Output/"+df.format(date)+"outputURLs")

      counts2
        .repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/URLs/URLS")
        //.save("Output/URLs/"+df.format(date)+"URLS")
    }
  }

  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}