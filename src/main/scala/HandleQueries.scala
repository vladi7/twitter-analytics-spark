import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, SparkSession}

import scala.sys.process.{ProcessLogger, stderr, stdout, _}
import scala.util.matching.Regex
//object mainObject{

//  def main(args: Array[String]): Unit ={
//    val hq = new HandleQueries
//    hq.analyze("hdfs://localhost:9000/tweets.json", "test")
//  }
class HandleQueries {

  object Analyzer{
    def callPython(args: Array[String]): String = {

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

    def top5Hashtags(tweets:DataFrame, ss: SparkSession): Unit ={
      import ss.implicits._

      //Get hashtags and work on data to just have a table of hashtags
      val dataset = tweets.select($"id", $"text", $"entities.hashtags", $"entities.urls", $"favorite_count" as "likes").as[Tweet] //(Encoders.product[SimpleTuple])
      println("DATASET:")
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
    def top5URLs(tweets:DataFrame, ss: SparkSession): Unit ={
      import ss.implicits._

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
    def top5HashtagsFollowers(tweets:DataFrame, ss: SparkSession): Unit = {
      import ss.implicits._
      val followers = ss.sql("SELECT entities.hashtags.text , user.followers_count as count FROM tweetswithschema order by  user.followers_count desc")
      followers.filter($"count".isNotNull).show()
      val newfollowers = followers.withColumn("newtext", concat_ws(" ", $"text"))
      val newFilteredfollowers = newfollowers.filter(!($"newtext" === ""))
      newFilteredfollowers.createOrReplaceTempView("newFilteredfollowers")

      val followersFinal = ss.sql("SELECT newtext, count  FROM newFilteredfollowers")

      followersFinal.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/Followers/Followers")
    }
    def top5URLsFollowers(tweets:DataFrame, ss: SparkSession): Unit = {
      import ss.implicits._
      val followers = ss.sql("SELECT entities.urls.expanded_url , user.followers_count as count FROM tweetswithschema order by  user.followers_count desc")
      followers.filter($"count".isNotNull).show()
      val newfollowers = followers.withColumn("newtext", concat_ws(" ", $"expanded_url"))
      val newFilteredfollowers = newfollowers.filter(!($"newtext" === ""))
      newFilteredfollowers.createOrReplaceTempView("newFilteredfollowers")

      val followersFinal = ss.sql("SELECT newtext, count  FROM newFilteredfollowers")

      followersFinal.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/Followers/Followers")
    }
    def sentimentCalculations(tweets:DataFrame, ss: SparkSession): Unit = {
      import ss.implicits._

      //Get hashtags and work on data to just have a table of hashtags
     // val dataset = tweets.select($"id", $"text", $"entities.hashtags", $"entities.urls", $"favorite_count" as "likes").as[Tweet] //(Encoders.product[SimpleTuple])
     // println("DATASET:")
     // dataset.show(10)
      val hashtags = ss.sql("SELECT text FROM tweetswithschema")
      hashtags.filter($"text".isNotNull).show()
      hashtags.createOrReplaceTempView("textOfTweet")

      //val newHashtags = hashtags.withColumn("newtext", concat_ws(" ", $"text"))
      val newFilteredHashtags = hashtags.filter(!($"text" === ""))
      newFilteredHashtags.createOrReplaceTempView("textfiltered")

      val hashtagsFinal: DataFrame = ss.sql("SELECT text FROM textfiltered")
      hashtagsFinal.show(10)
//
//      //Run a word count on hashtags and urls
//
//      // val pattern1 = new Regex("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)")
       val stopWordsList =ss.sparkContext.broadcast(Model.StopwordsLoader.loadStopWords("/StopWords.txt"))
      val pattern1 = new Regex("^\\s*[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*\\s*$")
      //Model.createAndSaveNBModel(ss,stopWordsList )

      val model = NaiveBayesModel.load(ss.sparkContext,"FilesForModel/model" )
      //Model.SentimentAnalyzer.computeSentiment("hello world",stopWordsList,model)
//      //text.rdd.take(10).foreach(println)
     // val countsmapped: Dataset[String] = hashtagsFinal.flatMap(sqlRow => (pattern1 findAllIn sqlRow(0).toString).toList)
     val toSentiment = (n: java.lang.String) => Model.SentimentAnalyzer.computeSentiment(n,stopWordsList, model)
      val dataset = hashtagsFinal.as[String]
     val countsreduced: KeyValueGroupedDataset[String, String] = dataset.groupByKey(toSentiment)
      val counts = countsreduced.count().orderBy($"value".desc)
      counts.show()

//      val date = new Date
//      val df = new SimpleDateFormat("yyyyMMddHHmmss")
//      df.setTimeZone(TimeZone.getTimeZone("America/Chicago"))
      counts.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/Sentiment/Sentiment")
//      //.save("Output/Hashtags/"+df.format(date)+"Hashtags")
    }
  }

  def analyze(args: Array[String]):String = {



    println("HELLO:")

    val code = Analyzer.callPython(args)
    if(code.contains("Failure")){
      throw new Exception("Invalid parameters, please check if you are missing any or double check your keys")
    }

    val ss: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    import ss.implicits._


    val data = ss.read.text("hdfs://localhost:9000/tweetsnew1.json").as[String]
    data.printSchema()
    data.createOrReplaceTempView("tweets")
    val text = ss.sql("SELECT * FROM tweets")
    val tweets: DataFrame = ss.read.json(ss.sql("SELECT * FROM tweets").as[String])
    tweets.createOrReplaceTempView("tweetswithschema")

    //println("TWEETSWITHSCHEMA:")
    //tweets.printSchema()

    if(args(7).equalsIgnoreCase("Top 5 Word Count Hashtags")) {
      Analyzer.top5Hashtags(tweets, ss)
    }
    //Get urls and work on data to just have a table of urls
    if(args(7).equalsIgnoreCase("Top 5 Word Count URLs")) {
      Analyzer.top5URLs(tweets, ss)
    }
    if(args(7).equalsIgnoreCase("Top Influencial Hashtags(hashtags sorted by followers of a user)")) {
      Analyzer.top5HashtagsFollowers(tweets, ss)
    }
    if(args(7).equalsIgnoreCase("Top Influencial URLs(urls sorted by followers of a user)")) {
      Analyzer.top5URLsFollowers(tweets, ss)
    }
    if(args(7).equalsIgnoreCase("Sentiments Piechart")) {
      Analyzer.sentimentCalculations(tweets, ss)
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

    if(action.equalsIgnoreCase("Top 5 Word Count Hashtags")) {
      Analyzer.top5Hashtags(tweets, ss)

    }
    //Get urls and work on data to just have a table of urls
    if(action.equalsIgnoreCase("Top 5 Word Count URLs")) {
      Analyzer.top5URLs(tweets, ss)

    }
    if(action.equalsIgnoreCase("Top Influencial Hashtags(hashtags sorted by followers of a user)")) {
      Analyzer.top5HashtagsFollowers(tweets, ss)

    }
    if(action.equalsIgnoreCase("Top Influencial URLs(urls sorted by followers of a user)")) {
      Analyzer.top5URLsFollowers(tweets, ss)
    }
    if(action.equalsIgnoreCase("Sentiments Piechart")) {
      Analyzer.sentimentCalculations(tweets, ss)
    }
    return "Success"

  }
}
//}