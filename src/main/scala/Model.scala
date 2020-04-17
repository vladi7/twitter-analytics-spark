/*
Inspired by https://github.com/P7h/Spark-MLlib-Twitter-Sentiment-Analysis
 */


import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random


// spark-submit --class "org.p7h.spark.sentiment.mllib.SparkNaiveBayesModelCreator" --master spark://spark:7077 spark-streaming-corenp-mllib-tweet-sentiment-assembly-0.1.jar
object Model {

//  def main(args: Array[String]) {
//    val ss = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
//
//    val stopWordsList =ss.sparkContext.broadcast(StopwordsLoader.loadStopWords("/StopWords.txt"))
//   // createAndSaveNBModel(ss, stopWordsList)
//   // validateAccuracyOfNBModel(ss, stopWordsList)
//   //
//    val model = NaiveBayesModel.load(ss.sparkContext,"FilesForModel/model" )
//    println("Sentiment:"+SentimentAnalyzer.computeSentiment("clever dwarf",stopWordsList,model))
//
//  }

  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }


  def createSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    val sc = SparkContext.getOrCreate(conf)
    sc
  }


  def createAndSaveNBModel(sc: SparkSession, stopWordsList: Broadcast[List[String]]): Unit = {
    val tweetsDF: DataFrame = loadSentiment140File(sc, "FilesForModel/training.1600000.processed.noemoticon.csv")

    val labeledRDD = tweetsDF.select("polarity", "status").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val tweetInWords: Seq[String] = SentimentAnalyzer.getBarebonesTweetText(tweet, stopWordsList.value)
        LabeledPoint(polarity, SentimentAnalyzer.transformFeatures(tweetInWords))
    }
    labeledRDD.cache()

    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
    naiveBayesModel.save(sc.sparkContext, "FilesForModel/model")
  }


  def validateAccuracyOfNBModel(sc: SparkSession, stopWordsList: Broadcast[List[String]]): Unit = {
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc.sparkContext, "FilesForModel/model")

    val tweetsDF: DataFrame = loadSentiment140File(sc, "FilesForModel/training.1600000.processed.noemoticon.csv")
    val actualVsPredictionRDD = tweetsDF.select("polarity", "status").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val tweetText = replaceNewLines(tweet)
        val tweetInWords: Seq[String] = SentimentAnalyzer.getBarebonesTweetText(tweetText, stopWordsList.value)
        (polarity.toDouble,
          naiveBayesModel.predict(SentimentAnalyzer.transformFeatures(tweetInWords)),
          tweetText)
    }
    val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()
    /*actualVsPredictionRDD.cache()
    val predictedCorrect = actualVsPredictionRDD.filter(x => x._1 == x._2).count()
    val predictedInCorrect = actualVsPredictionRDD.filter(x => x._1 != x._2).count()
    val accuracy = 100.0 * predictedCorrect.toDouble / (predictedCorrect + predictedInCorrect).toDouble*/
    println(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")
    saveAccuracy(sc, actualVsPredictionRDD)
  }


  def loadSentiment140File(ss: SparkSession, sentiment140FilePath: String): DataFrame = {

    val tweetsDF = ss.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(sentiment140FilePath)
      .toDF("polarity", "id", "date", "query", "user", "status")

    // Drop the columns we are not interested in.
    tweetsDF.drop("id").drop("date").drop("query").drop("user")
  }


  def saveAccuracy(ss: SparkSession, actualVsPredictionRDD: RDD[(Double, Double, String)]): Unit = {

    import ss.implicits._
    val actualVsPredictionDF = actualVsPredictionRDD.toDF("Actual", "Predicted", "Text")
    actualVsPredictionDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      // Compression codec to compress while saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save("Output/Accuracy/")
  }
  import scala.io.Source


  object StopwordsLoader {

    def loadStopWords(stopWordsFileName: String): List[String] = {
      print(getClass)

      Source.fromFile("src/main/resources/StopWords.txt").getLines().toList
    }
  }
  object SentimentAnalyzer {



    def computeSentiment(text: String, stopWordsList: Broadcast[List[String]], model: NaiveBayesModel): String = {
      val randomInt = new Random()

      val tweetInWords: Seq[String] = getBarebonesTweetText(text, stopWordsList.value)
      val polarity = model.predict(SentimentAnalyzer.transformFeatures(tweetInWords))
      val randomNumber = randomInt.nextInt(10)

      if(randomNumber>7&&polarity == 0)//adding randomness
        {
          normalizeMLlibSentiment(2)//adding randomness
        }
      else if(randomNumber<7&&polarity == 0){
        normalizeMLlibSentiment(0)
      } else if(randomNumber>7&&polarity == 4)//adding randomness
      {
        normalizeMLlibSentiment(2)//adding randomness
      }
      else if(randomNumber<7&&polarity == 4){
        normalizeMLlibSentiment(4)
      }
      else{
        normalizeMLlibSentiment(4)
      }

    }


    def normalizeMLlibSentiment(sentiment: Double) = {
      sentiment match {
        case x if x == 0 => "Negative" // negative
        case x if x == 2 => "Neutral" // neutral
        case x if x == 4 => "Positive" // positive
        case _ => "Neutral" // if cant figure the sentiment, term it as neutral
      }
    }


    def getBarebonesTweetText(tweetText: String, stopWordsList: List[String]): Seq[String] = {
      //Remove URLs, RT, MT and other redundant chars / strings from the tweets.
      tweetText.toLowerCase()
        .replaceAll("\n", "")
        .replaceAll("rt\\s+", "")
        .replaceAll("\\s+@\\w+", "")
        .replaceAll("@\\w+", "")
        .replaceAll("\\s+#\\w+", "")
        .replaceAll("#\\w+", "")
        .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
        .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
        .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
        .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
        .split("\\W+")
        .filter(_.matches("^[a-zA-Z]+$"))
        .filter(!stopWordsList.contains(_))
      //.fold("")((a,b) => a.trim + " " + b.trim).trim
    }

    val hashingTF = new HashingTF()

    def transformFeatures(tweetText: Seq[String]): Vector = {
      hashingTF.transform(tweetText)
    }
  }
}
