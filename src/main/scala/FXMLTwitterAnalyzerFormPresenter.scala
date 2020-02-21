//https://github.com/scalafx/ProScalaFX/blob/master/src/proscalafx/ch07/ChartApp8.scala
//how to draw a graph


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
case class Tweet(id: Long,
                 text: String,
                 hashTags: String,
                 urls: String,
                 likes: Long)
@sfxml
class FXMLTwitterAnalyzerFormPresenter(private val consumerKeyField: TextField,
                                       private val consumerSecretField: TextField,
                                       private val accessSecretField: TextField,
                                       private val accessTokenField: TextField,
                                       private val wordsField: TextField,
                                       private val languagesField: TextField,
                                       private val numberOfTweetsField: TextField,
                                       private val actionChoiceBox: ChoiceBox[String],
                                       private val additionalInfoTextArea: TextArea,
                                       private val grid: GridPane,
                                       private val lineChart: LineChart[Number, Number],
                                       private val barChartHashtagsURLsCount: BarChart[String, Number],
                                       private val buttonGraph: Button,
                                       private val buttonSave: Button,
                                       private val buttonSubmit: Button,
                                       private val progressBar: ProgressBar,
                                       private val locationOfTheDataset: TextField) {

  def handleSubmit(event: ActionEvent): Unit = {
    barChartHashtagsURLsCount.getData.clear()

    if(!locationOfTheDataset.text.value.isEmpty)
      {
        val f = Future {
          analyze(locationOfTheDataset.text.value, actionChoiceBox.getValue())
          "Success"
        }
        f.onComplete {
          case Success(value) => successAction()
          case Failure(e) => failureAction(e.getMessage)

        }
      }
    else {
      val tweetStream = new Array[String](8)
      tweetStream(0) = consumerKeyField.text.value
      tweetStream(1) = consumerSecretField.text.value
      tweetStream(2) = accessSecretField.text.value
      tweetStream(3) = accessTokenField.text.value
      tweetStream(4) = languagesField.text.value
      tweetStream(5) = wordsField.text.value
      tweetStream(6) = numberOfTweetsField.text.value
      tweetStream(7) = actionChoiceBox.getValue()


      //additionalInfoTextArea.text = "Job has been completed. Please look in the terminal for its output and in folder output for its . Please press \"Graph\" to see the graph about the job. Please note, that after you hit graph you will have to resubmit the job to obtain the files with full result as they will be deleted. The same will happen if you press clear."
      val f = Future {
        analyze(tweetStream)
        "Success"
      }
      f.onComplete {
        case Success(value) => successAction()
        case Failure(e) => failureAction(e.getMessage)

      }
    }
    actionChoiceBox.setDisable(true)
    buttonSubmit.setDisable(true)
    barChartHashtagsURLsCount.visible = false
    progressBar.visible = true

    progressBar.setProgress( ProgressIndicator.IndeterminateProgress)
  }

  def handleClear(event: ActionEvent): Unit = {
    //consumerKeyField.text = ""
    //consumerSecretField.text = ""
   // accessSecretField.text = ""
   // accessTokenField.text = ""
    languagesField.text = ""
    wordsField.text = ""
    numberOfTweetsField.text = ""
    additionalInfoTextArea.text = "After clicking \"Submit\", please wait for the job to complete. After that, you will see a message in this box about that."
    actionChoiceBox.setDisable(false)
    buttonGraph.setDisable(true)
    buttonSubmit.setDisable(false)

    val file1 = new File("Output/Hashtags/")
    val file2 = new File("Output/URLs/")

    FileUtils.cleanDirectory(file1);
    FileUtils.cleanDirectory(file2);
    barChartHashtagsURLsCount.visible = false
    buttonSave.setDisable(true)


  }
  def handleSave(event: ActionEvent): Unit = {
    val date = new Date
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    df.setTimeZone(TimeZone.getTimeZone("America/Chicago"))
    additionalInfoTextArea.text = "Graph was saved in the ProjectFolderPath/OutputGraphs! After clicking \"Submit\", please wait for the job to complete. After that, you will see a message in this box about that."
    buttonSave.setDisable(true)


    val image =  barChartHashtagsURLsCount.snapshot(new SnapshotParameters(), null)

    import javax.imageio.ImageIO
    import java.io.IOException
    val file = new File("OutputGraphs/"+df.format(date)+"chart.png")

    try ImageIO.write(SwingFXUtils.fromFXImage(image, null), "png", file)
    catch {
      case e: IOException =>

      // TODO: handle exception here
    }

  }
  // val newNames = Seq("name", "count")
  //al dfRenamed = dataFromFile.toDF(newNames: _*)

  def handleGraph(event: ActionEvent): Unit = {


    //      lineChart.title = "Sentiment Graph Test"
    //
    //      // defining a series
    //      val data = ObservableBuffer(Seq(
    //        (1, 23),
    //        (2, 14),
    //        (3, 15),
    //        (4, 24),
    //        (5, 34),
    //        (6, 36),
    //        (7, 22),
    //        (8, 45),
    //        (9, 43),
    //        (10, 17),
    //        (11, 29),
    //        (12, 25)
    //      ) map { case (x, y) => XYChart.Data[Number, Number](x, y) })
    //
    //      val series = XYChart.Series[Number, Number]("test", data)
    //
    //      lineChart.getData.add(series)
    // lineChart.visible = true

    val actionValue = actionChoiceBox.getValue()


    val ss = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    if (actionValue.equals("Top 5 Word Count Hashtags")) {
      val dataFromFile = ss.read.format("csv").option("header", "true").load("Output/Hashtags/**/*csv")
      val arrayOfRowsHashtags = dataFromFile.take(5)
      val arrayOfRowsHashtagsNoBraces = for (e <- arrayOfRowsHashtags) yield e.toString().replaceAll("[\\[\\]]", "")
      val arrayOfRowsHashtagsFinal = for (e <- arrayOfRowsHashtagsNoBraces) yield e.toString() split (",")


      val dataHashtags = ObservableBuffer(Seq(
        (arrayOfRowsHashtagsFinal(0)(0).toString, arrayOfRowsHashtagsFinal(0)(1).toInt),
        (arrayOfRowsHashtagsFinal(1)(0).toString, arrayOfRowsHashtagsFinal(1)(1).toInt),
        (arrayOfRowsHashtagsFinal(2)(0).toString, arrayOfRowsHashtagsFinal(2)(1).toInt),
        (arrayOfRowsHashtagsFinal(3)(0).toString, arrayOfRowsHashtagsFinal(3)(1).toInt),
        (arrayOfRowsHashtagsFinal(4)(0).toString, arrayOfRowsHashtagsFinal(4)(1).toInt)

      ) map { case (x, y) => XYChart.Data[String, Number](x, y) })

      val seriesHashTags = XYChart.Series[String, Number]("Hashtags", dataHashtags)
      barChartHashtagsURLsCount.getData.add(seriesHashTags)
      barChartHashtagsURLsCount.visible = true
      val file1 = new File("Output/Hashtags/")
      FileUtils.cleanDirectory(file1);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      return
    }
    if (actionValue.equals("Top 5 Word Count URLs")) {

      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/URLs/**/*csv")

      val arrayOfRowsURLs = dataFromFileURLs.take(5)
      val arrayOfRowsURLsNoBraces = for (e <- arrayOfRowsURLs) yield e.toString().replaceAll("[\\[\\]]", "")
      val arrayOfRowsURLsFinal = for (e <- arrayOfRowsURLsNoBraces) yield e.toString() split (",")
      arrayOfRowsURLsFinal(0)(0).toString.substring(0, 5)

      val dataURLs = ObservableBuffer(Seq(
        (arrayOfRowsURLsFinal(0)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(0)(0).toString.length, 30)), arrayOfRowsURLsFinal(0)(1).toInt),
        (arrayOfRowsURLsFinal(1)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(1)(0).toString.length, 30)), arrayOfRowsURLsFinal(1)(1).toInt),
        (arrayOfRowsURLsFinal(2)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(2)(0).toString.length, 30)), arrayOfRowsURLsFinal(2)(1).toInt),
        (arrayOfRowsURLsFinal(3)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(3)(0).toString.length, 30)), arrayOfRowsURLsFinal(3)(1).toInt),
        (arrayOfRowsURLsFinal(4)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(4)(0).toString.length, 30)), arrayOfRowsURLsFinal(4)(1).toInt)

      ) map { case (x, y) => XYChart.Data[String, Number](x, y) })

      val seriesURLs = XYChart.Series[String, Number]("URLs", dataURLs)
      barChartHashtagsURLsCount.getData.add(seriesURLs)
      barChartHashtagsURLsCount.visible = true
      val file2 = new File("Output/URLs/")
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "
      return
    }


    if (actionValue.equals("Both")) {
      val dataFromFile = ss.read.format("csv").option("header", "true").load("Output/Hashtags/**/*csv")
      val arrayOfRowsHashtags = dataFromFile.take(5)
      val arrayOfRowsHashtagsNoBraces = for (e <- arrayOfRowsHashtags) yield e.toString().replaceAll("[\\[\\]]", "")
      val arrayOfRowsHashtagsFinal = for (e <- arrayOfRowsHashtagsNoBraces) yield e.toString() split (",")


      val dataHashtags = ObservableBuffer(Seq(
        (arrayOfRowsHashtagsFinal(0)(0).toString, arrayOfRowsHashtagsFinal(0)(1).toInt),
        (arrayOfRowsHashtagsFinal(1)(0).toString, arrayOfRowsHashtagsFinal(1)(1).toInt),
        (arrayOfRowsHashtagsFinal(2)(0).toString, arrayOfRowsHashtagsFinal(2)(1).toInt),
        (arrayOfRowsHashtagsFinal(3)(0).toString, arrayOfRowsHashtagsFinal(3)(1).toInt),
        (arrayOfRowsHashtagsFinal(4)(0).toString, arrayOfRowsHashtagsFinal(4)(1).toInt)

      ) map { case (x, y) => XYChart.Data[String, Number](x, y) })

      val seriesHashTags = XYChart.Series[String, Number]("Hashtags", dataHashtags)

      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/URLs/**/*csv")

      val arrayOfRowsURLs = dataFromFileURLs.take(5)
      val arrayOfRowsURLsNoBraces = for (e <- arrayOfRowsURLs) yield e.toString().replaceAll("[\\[\\]]", "")
      val arrayOfRowsURLsFinal = for (e <- arrayOfRowsURLsNoBraces) yield e.toString() split (",")
      arrayOfRowsURLsFinal(0)(0).toString.substring(0, 5)

      val dataURLs = ObservableBuffer(Seq(
        (arrayOfRowsURLsFinal(0)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(0)(0).toString.length, 30)), arrayOfRowsURLsFinal(0)(1).toInt),
        (arrayOfRowsURLsFinal(1)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(1)(0).toString.length, 30)), arrayOfRowsURLsFinal(1)(1).toInt),
        (arrayOfRowsURLsFinal(2)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(2)(0).toString.length, 30)), arrayOfRowsURLsFinal(2)(1).toInt),
        (arrayOfRowsURLsFinal(3)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(3)(0).toString.length, 30)), arrayOfRowsURLsFinal(3)(1).toInt),
        (arrayOfRowsURLsFinal(4)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(4)(0).toString.length, 30)), arrayOfRowsURLsFinal(4)(1).toInt)

      ) map { case (x, y) => XYChart.Data[String, Number](x, y) })

      val seriesURLs = XYChart.Series[String, Number]("URLs", dataURLs)
      barChartHashtagsURLsCount.getData.addAll(seriesHashTags, seriesURLs)
      barChartHashtagsURLsCount.visible = true


      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. You can also run the job again with \"Submit\" button and switch parameters above."

      val file1 = new File("Output/Hashtags/")
      val file2 = new File("Output/URLs/")

      FileUtils.cleanDirectory(file1);
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      buttonSave.setDisable(false)

    }
  }

  def onClose(event: ActionEvent) {
    System.exit(0)
  }


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
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  def successAction():Unit={
    additionalInfoTextArea.text = "Job has been completed. Please look in the terminal for its output and in folder output for its . Please press \"Graph\" to see the graph about the job. Please note, that after you hit graph you will have to resubmit the job to obtain the files with full result as they will be deleted. The same will happen if you press clear."
    progressBar.visible = false
    buttonGraph.setDisable(false)

  }
  def failureAction(message : String):Unit={
    additionalInfoTextArea.text = message
    progressBar.visible = false
    actionChoiceBox.setDisable(false)
    buttonGraph.setDisable(true)
    buttonSubmit.setDisable(false)
  }
}

