//http://blog.madhukaraphatak.com/introduction-to-spark-two-part-2
//reference for word count done efficiently


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
import org.apache.spark.sql.functions.{col,concat_ws}


case class Tweet(id: Long,
                 text: String,
                 hashTags: String,
                 urls: String,
                 likes: Long)

object PrintTweets {
  def main(args: Array[String]) {

    def callPython(): Unit = {
      val result = "python TweetStreamer.py consumer_key consumer_secret access_token access_secret [en,ru] [I,a,the,love,thank,happy,great] 100" ! ProcessLogger(stdout append _, stderr append _)
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


    //Get hashtags and work on data to just have a table of hashtags
    val dataset = tweets.select($"id", $"text", $"entities.hashtags", $"entities.urls", $"favorite_count" as "likes").as[Tweet]//(Encoders.product[SimpleTuple])
    dataset.show(10)

    val hashtags = ss.sql("SELECT entities.hashtags.text FROM tweetswithschema")
    hashtags.filter($"text".isNotNull).show()

    val newHashtags = hashtags.withColumn("newtext", concat_ws(" ", $"text"))
    val newFilteredHashtags = newHashtags.filter(!($"newtext"===""))
    newFilteredHashtags.createOrReplaceTempView("hashtagsfiltered")

    val hashtagsFinal = ss.sql("SELECT newtext FROM hashtagsfiltered")




    //Get urls and work on data to just have a table of urls

    val urls = ss.sql("SELECT entities.urls.expanded_url FROM tweetswithschema")
    urls.filter($"expanded_url".isNotNull).show()

    val newUrls = urls.withColumn("newtext", concat_ws(" ", $"expanded_url"))
    val newFilteredUrls = newUrls.filter(!($"newtext"===""))
    newFilteredUrls.createOrReplaceTempView("urlstagsfiltered")


    val urlssFinal = ss.sql("SELECT newtext FROM urlstagsfiltered")


    hashtags.createOrReplaceTempView("hashtagtext")



    //Run a word count on hashtags and urls

   // val pattern1 = new Regex("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)")

    val pattern1 = new Regex("^\\s*[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*\\s*$")

    //text.rdd.take(10).foreach(println)
    val countsmapped = hashtagsFinal.flatMap(sqlRow => (pattern1 findAllIn sqlRow(0).toString).toList)
    val countsreduced = countsmapped.groupByKey(_.toLowerCase)
    val counts = countsreduced.count().orderBy($"count(1)".desc)
    counts.show()


    val pattern2 = new Regex("(www|http)\\S+")

     val countsmapped2 = urlssFinal.flatMap(sqlRow => (pattern2 findAllIn sqlRow(0).toString).toList)
     val countsreduced2 = countsmapped2.groupByKey(_.toLowerCase)
     val counts2 = countsreduced2.count().orderBy($"count(1)".desc)

    counts2.show()

  }

  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}