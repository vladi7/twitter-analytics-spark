import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat_ws

import scala.sys.process.{ProcessLogger, stderr, stdout}
import scala.util.matching.Regex
import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import java.util.Locale.Category

import scalafx.event.ActionEvent
import scalafx.scene.control.{Button, ChoiceBox, ProgressBar, ProgressIndicator, TextArea, TextField}
import scalafx.scene.layout.GridPane
import scalafxml.core.macros.sfxml
import com.lynden.gmapsfx.GoogleMapView
import org.apache.commons.io.FileUtils
import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.spark_project.guava.collect.Iterables
import scalafx.scene.chart.{BarChart, CategoryAxis, LineChart, NumberAxis, XYChart}
import scalafx.collections.ObservableBuffer
import org.apache.commons.io.FileUtils.cleanDirectory
import org.apache.spark.sql.functions.concat_ws
import scalafx.embed.swing.SwingFXUtils
import scalafx.scene.SnapshotParameters
import scalafx.scene.image.WritableImage

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.sys.process.{ProcessLogger, stderr, stdout}
import scala.util.matching.Regex
import scala.util.{Failure, Success}
import sys.process._
class HandleQueries {
  def analyze(args: Array[String]):String = {

    def callPython(): String = {

      // print("python TweetStreamer.py "+ args(0) + " "+args(1)+" "+ args(3)+" "+args(2)+" "+args(4) +" "+ args(5)+" "+args(6))
      //      System.exit(0)
      //val result = "python TweetStreamer.py DBBLXnPte2RbTlX0Oh1DhQegp ZyCfhnPeaLYK7TmiKweJec49WhjOHce4yWlkEueT5FSX39XOPX 1220900648968912897-GxoA6gr81750WqMLSwmJbzLmBL0ryG WG5vi86sVpnjT4EwgIzaAHVGRgYsfWvSRsydgWaDogZ3A [en,ru] [I,a,the,love,thank,happy,great] 1000" ! ProcessLogger(stdout append _, stderr append _)

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
        return "Failure"
      }
      print("+++++++++++++++++")
      println("stdout: " + stdout)
      println("stderr: " + stderr)
      return "Success"
    }


    val code = callPython()
    if(code.contains("Failure")){
      throw new Exception("Invalid parameters, please check if you are missing any or double check your keys")
    }

    val ss = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    import ss.implicits._


    val data = ss.read.text("hdfs://localhost:9000/tweetsnew1.json").as[String]
    data.printSchema()
    data.createOrReplaceTempView("tweets")
    val text = ss.sql("SELECT * FROM tweets")
    val tweets = ss.read.json(ss.sql("SELECT * FROM tweets").as[String])
    tweets.createOrReplaceTempView("tweetswithschema")



    import java.text.SimpleDateFormat
    import java.util.TimeZone

    if(args(7).equalsIgnoreCase("Top 5 Word Count Hashtags")||args(7).equalsIgnoreCase("Both")) {
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


      val date = new Date
      val df = new SimpleDateFormat("yyyyMMddHHmmss")
      df.setTimeZone(TimeZone.getTimeZone("America/Chicago"))
      counts.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/Hashtags/Hashtags")
      //.save("Output/Hashtags/"+df.format(date)+"Hashtags")

    }
    //Get urls and work on data to just have a table of urls
    if(args(7).equalsIgnoreCase("Top 5 Word Count URLs")||args(7).equalsIgnoreCase("Both")) {

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
    return "Success"

  }
  def analyze(locationOfTheFile : String, action : String):String = {


    val ss = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    import ss.implicits._


    val data = ss.read.text(locationOfTheFile).as[String]
    data.printSchema()
    data.createOrReplaceTempView("tweets")
    val text = ss.sql("SELECT * FROM tweets")
    val tweets = ss.read.json(ss.sql("SELECT * FROM tweets").as[String])
    tweets.createOrReplaceTempView("tweetswithschema")



    import java.text.SimpleDateFormat
    import java.util.TimeZone

    if(action.equalsIgnoreCase("Top 5 Word Count Hashtags")||action.equalsIgnoreCase("Both")) {
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


      val date = new Date
      val df = new SimpleDateFormat("yyyyMMddHHmmss")
      df.setTimeZone(TimeZone.getTimeZone("America/Chicago"))
      counts.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/Hashtags/Hashtags")
      //.save("Output/Hashtags/"+df.format(date)+"Hashtags")

    }
    //Get urls and work on data to just have a table of urls
    if(action.equalsIgnoreCase("Top 5 Word Count URLs")||action.equalsIgnoreCase("Both")) {

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
    return "Success"

  }
}
