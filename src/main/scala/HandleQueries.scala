import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, SparkSession}

import scala.sys.process.{ProcessLogger, stderr, stdout, _}
import scala.util.matching.Regex
//object mainObject{
//
//  def main(args: Array[String]): Unit ={
//    val hq = new HandleQueries
//    hq.analyze("hdfs://localhost:9000/tweetsnew1.json", "Timeline of Languages of Tweets")
//  }
class HandleQueries {

  object Analyzer{
    def callPython(args: Array[String]): String = {

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




      val pattern1 = new Regex("^\\s*[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*\\s*$")

      val countsmapped = hashtagsFinal.flatMap(sqlRow => (pattern1 findAllIn sqlRow(0).toString).toList)
      val countsreduced = countsmapped.groupByKey(_.toLowerCase)
      val counts = countsreduced.count().orderBy($"count(1)".desc)
      counts.show()


      val df = new SimpleDateFormat("yyyyMMddHHmmss")
      df.setTimeZone(TimeZone.getTimeZone("America/Chicago"))
      counts.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/Hashtags/Hashtags")



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


      counts2
        .repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/URLs/URLS")


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


      val hashtags = ss.sql("SELECT text FROM tweetswithschema")
      hashtags.filter($"text".isNotNull).show()
      hashtags.createOrReplaceTempView("textOfTweet")

      val newFilteredHashtags = hashtags.filter(!($"text" === ""))
      newFilteredHashtags.createOrReplaceTempView("textfiltered")

      val hashtagsFinal: DataFrame = ss.sql("SELECT text FROM textfiltered")
      hashtagsFinal.show(10)

       val stopWordsList =ss.sparkContext.broadcast(Model.StopwordsLoader.loadStopWords("/StopWords.txt"))
      val pattern1 = new Regex("^\\s*[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*\\s*$")

      val model = NaiveBayesModel.load(ss.sparkContext,"FilesForModel/model" )

     val toSentiment = (n: java.lang.String) => Model.SentimentAnalyzer.computeSentiment(n,stopWordsList, model)
      val dataset = hashtagsFinal.as[String]
     val countsreduced: KeyValueGroupedDataset[String, String] = dataset.groupByKey(toSentiment)
      val counts = countsreduced.count().orderBy($"value".desc)
      counts.show()

      counts.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/Sentiment/Sentiment")
    }
    def sentimentCalculationsBubbleChart(tweets:DataFrame, ss: SparkSession): Unit = {
      import ss.implicits._

      val hashtags = ss.sql("SELECT text as tweettext, entities.hashtags.text, user.followers_count as followers, created_at as time, user.statuses_count as statuses FROM tweetswithschema")
      hashtags.filter($"tweettext".isNotNull).show()
      hashtags.filter($"text".isNotNull).show()
      hashtags.filter($"followers".isNotNull)
      hashtags.createOrReplaceTempView("textOfTweet")

     val newHashtags =  hashtags.withColumn("hashtag", concat_ws(" ", $"text"))

      val newFilteredHashtags1 = newHashtags.filter(!($"tweettext" === ""))
      println("1")
      newFilteredHashtags1.show()
      val newFilteredHashtags2 = newFilteredHashtags1.filter(!($"hashtag" === ""))
      println("2")

      newFilteredHashtags2.show()

      newFilteredHashtags2.createOrReplaceTempView("textfiltered")
      val hashtagsFinal: DataFrame = ss.sql("SELECT tweettext FROM textfiltered")

      val hashtagsFinal2: DataFrame = ss.sql("SELECT tweettext, hashtag, followers, time, statuses FROM textfiltered")
      hashtagsFinal.show(10)
      import org.apache.spark.sql.functions.udf




      val stopWordsList =ss.sparkContext.broadcast(Model.StopwordsLoader.loadStopWords("/StopWords.txt"))

      val model = NaiveBayesModel.load(ss.sparkContext,"FilesForModel/model" )

      val toSentiment = (n: java.lang.String) => Model.SentimentAnalyzer.computeSentiment2(n,stopWordsList, model)
      val sentiment = udf(toSentiment)
      val DF: DataFrame = hashtagsFinal2.withColumn("sentiment",sentiment('tweettext))
      import org.apache.spark.sql.functions._

      val finalDF = DF.select(col("textfiltered.followers"),col("textfiltered.statuses"),col("sentiment"),substring(col("textfiltered.time"), 15, 2).as("time"))
      finalDF.show(10)

      finalDF.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/SentimentBubbleChart/SentimentBubbleChart")


    }

    def languagePieChart(tweets:DataFrame, ss: SparkSession): Unit = {
      import ss.implicits._

      val hashtags = ss.sql("SELECT lang FROM tweetswithschema")
      hashtags.filter($"lang".isNotNull).show()
      hashtags.createOrReplaceTempView("hashtagtext")

      val newFilteredHashtags = hashtags.filter(!($"lang" === ""))
      newFilteredHashtags.createOrReplaceTempView("hashtagsfiltered")

      val hashtagsFinal = ss.sql("SELECT lang FROM hashtagsfiltered")
      hashtagsFinal.show(10)


      val pattern1 = new Regex("^\\s*[A-Za-z0-9]+(?:\\s+[A-Za-z0-9]+)*\\s*$")

      val countsmapped = hashtagsFinal.flatMap(sqlRow => (pattern1 findAllIn sqlRow(0).toString).toList)
      val countsreduced = countsmapped.groupByKey(_.toLowerCase)
      val counts = countsreduced.count().orderBy($"count(1)".desc)
      counts.show()

      val date = new Date
      val df = new SimpleDateFormat("yyyyMMddHHmmss")
      df.setTimeZone(TimeZone.getTimeZone("America/Chicago"))
      counts.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/LanguagePieChart/LanguagePieChart")


    }
    def languagePieChartFollowers(tweets:DataFrame, ss: SparkSession): Unit = {
      import ss.implicits._

      val hashtags = ss.sql("SELECT lang,  user.followers_count as followers FROM tweetswithschema")
      hashtags.filter($"lang".isNotNull)
      hashtags.filter($"followers".isNotNull)

      hashtags.createOrReplaceTempView("hashtagtext")
      val newFilteredHashtags = hashtags.filter(!($"lang" === ""))
      newFilteredHashtags.createOrReplaceTempView("hashtagsfiltered")

      val hashtagsFinal = ss.sql("SELECT lang, followers FROM hashtagsfiltered")


      val countFollowersFinal: DataFrame = hashtagsFinal.groupBy("lang").sum()

      countFollowersFinal.show(10)


      countFollowersFinal.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/LanguageFollowersPieChart/LanguageFollowersPieChart")

    }
    def timelineOfSentiments(tweets:DataFrame, ss: SparkSession): Unit = {
      import ss.implicits._



      val hashtags = ss.sql("SELECT text as tweettext, created_at as time , user.followers_count as followers FROM tweetswithschema")
      hashtags.filter($"tweettext".isNotNull).show()
      hashtags.createOrReplaceTempView("textOfTweet")


      val newFilteredHashtags1 = hashtags.filter(!($"tweettext" === ""))
      println("1")
      newFilteredHashtags1.show()

      newFilteredHashtags1.createOrReplaceTempView("textfiltered")


      val hashtagsFinal: DataFrame = ss.sql("SELECT tweettext,time,followers FROM textfiltered")

      import org.apache.spark.sql.functions.udf



      val stopWordsList =ss.sparkContext.broadcast(Model.StopwordsLoader.loadStopWords("/StopWords.txt"))

      val model = NaiveBayesModel.load(ss.sparkContext,"FilesForModel/model" )

      val toSentiment = (n: java.lang.String) => Model.SentimentAnalyzer.computeSentiment(n,stopWordsList, model)
      val sentiment = udf(toSentiment)
      val DF: DataFrame = hashtagsFinal.withColumn("sentiment",sentiment('tweettext))
      import org.apache.spark.sql.functions._

      val finalDF = DF.select(col("textfiltered.followers"),col("sentiment"),substring(col("textfiltered.time"), 15, 2).as("time"))
      finalDF.show(10)


      finalDF.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/LineChartSentimentFollowers/LineChartSentimentFollowers")

    }

    def timelineOfLanguages(tweets:DataFrame, ss: SparkSession): Unit = {
      import ss.implicits._



      val hashtags = ss.sql("SELECT created_at as time , user.followers_count as followers, lang FROM tweetswithschema")
      hashtags.filter($"lang".isNotNull).show()

      hashtags.createOrReplaceTempView("textOfTweet")


      val newFilteredHashtags1 = hashtags.filter(!($"lang" === ""))

      newFilteredHashtags1.createOrReplaceTempView("textfiltered")


      val hashtagsFinal: DataFrame = ss.sql("SELECT lang,time,followers FROM textfiltered")


      import org.apache.spark.sql.functions._

      val finalDF = hashtagsFinal.select(col("textfiltered.lang"),col("textfiltered.followers"),substring(col("textfiltered.time"), 15, 2).as("time"))
      finalDF.show(10)


      finalDF.repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true").save("Output/timelineOfLanguages/timelineOfLanguages")

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
    if(args(7).equalsIgnoreCase("Sentiments Piechart(English Only)")) {
      Analyzer.sentimentCalculations(tweets, ss)
    }
    if(args(7).equalsIgnoreCase("Sentiment Bubblechart(English Only)")) {
      Analyzer.sentimentCalculationsBubbleChart(tweets, ss)
    }
    if(args(7).equalsIgnoreCase("Language Piechart Tweet Count")) {
      Analyzer.languagePieChart(tweets, ss)
    }
    if(args(7).equalsIgnoreCase("Language Piechart Influence(Followers)")) {
      Analyzer.languagePieChartFollowers(tweets, ss)
    }
    if(args(7).equalsIgnoreCase("Timeline of Sentiments of Tweets")) {
      Analyzer.timelineOfSentiments(tweets, ss)
    }
    if(args(7).equalsIgnoreCase("Timeline of Languages of Tweets")) {
      Analyzer.timelineOfLanguages(tweets, ss)
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
    if(action.equalsIgnoreCase("Sentiments Piechart(English Only)")) {
      Analyzer.sentimentCalculations(tweets, ss)
    }
    if(action.equalsIgnoreCase("Sentiment Bubblechart(English Only)")) {
      Analyzer.sentimentCalculationsBubbleChart(tweets, ss)
    }
    if(action.equalsIgnoreCase("Language Piechart Tweet Count")) {
      Analyzer.languagePieChart(tweets, ss)
    }
    if(action.equalsIgnoreCase("Language Piechart Influence(Followers)")) {
      Analyzer.languagePieChartFollowers(tweets, ss)
    }
    if(action.equalsIgnoreCase("Timeline of Sentiments of Tweets")) {
      Analyzer.timelineOfSentiments(tweets, ss)
    }
    if(action.equalsIgnoreCase("Timeline of Languages of Tweets")) {
      Analyzer.timelineOfLanguages(tweets, ss)
    }

    return "Success"

  }
//}
}