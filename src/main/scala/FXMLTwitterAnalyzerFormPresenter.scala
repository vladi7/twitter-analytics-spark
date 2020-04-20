//https://github.com/scalafx/ProScalaFX/blob/master/src/proscalafx/ch07/ChartApp8.scala
//https://github.com/scalafx/scalafx-ensemble/blob/master/src/main/scala/scalafx/ensemble/example/charts/EnsembleLineChart.scala
//Example were taken from above links to draw a graph.


import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, TimeZone}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scalafx.collections.ObservableBuffer
import scalafx.embed.swing.SwingFXUtils
import scalafx.event.ActionEvent
import scalafx.scene.SnapshotParameters
import scalafx.scene.chart._
import scalafx.scene.control._
import scalafx.scene.layout.GridPane
import scalafxml.core.macros.sfxml

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
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
                                       private val locationOfTheDataset: TextField,
                                       private val pieChart: PieChart,
                                       private val bubbleChart: BubbleChart[Number, Number],
                                       private val xaxis: NumberAxis,
                                       private val languagePieChart: PieChart,
                                      ) {
  def handleSubmit(event: ActionEvent): Unit = {
    barChartHashtagsURLsCount.getData.clear()
    pieChart.getData.clear
    bubbleChart.getData.clear
    languagePieChart.getData.clear
    lineChart.getData.clear
    if(!locationOfTheDataset.text.value.isEmpty)
      {
        val f = Future {
          val hq = new HandleQueries
          hq.analyze(locationOfTheDataset.text.value, actionChoiceBox.getValue())
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
        val hq = new HandleQueries
        hq.analyze(tweetStream)
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
    pieChart.visible = false
    languagePieChart.visible = false
    additionalInfoTextArea.text = "Analytics Job is in progress, please wait."
    bubbleChart.visible=false
    lineChart.visible = false
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
    pieChart.visible = false
    languagePieChart.visible = false

    buttonSave.setDisable(true)


  }
  def handleSave(event: ActionEvent): Unit = {
    val date = new Date
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    df.setTimeZone(TimeZone.getTimeZone("America/Chicago"))
    additionalInfoTextArea.text = "Graph was saved in the ProjectFolderPath/OutputGraphs! After clicking \"Submit\", please wait for the job to complete. After that, you will see a message in this box about that."
    buttonSave.setDisable(true)


    val image =  barChartHashtagsURLsCount.snapshot(new SnapshotParameters(), null)

    import java.io.IOException

    import javax.imageio.ImageIO
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
      barChartHashtagsURLsCount.setTitle("Top 5 Word Count Hashtags")
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
      barChartHashtagsURLsCount.setTitle("Top 5 Word Count URLs")

      return
    }

    if (actionValue.equals("Top Influencial Hashtags(hashtags sorted by followers of a user)")) {

      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/Followers/**/*csv")

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

      val seriesURLs = XYChart.Series[String, Number]("Hashtags posted by the people with the most followers", dataURLs)
      barChartHashtagsURLsCount.getData.add(seriesURLs)
      barChartHashtagsURLsCount.visible = true
      val file2 = new File("Output/Followers/")
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "
      barChartHashtagsURLsCount.setTitle("Top 5 Hashtags by Followers Of The Account")

      return
    }
    if (actionValue.equals("Top Influencial URLs(urls sorted by followers of a user)")) {

      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/Followers/**/*csv")

      val arrayOfRowsURLs = dataFromFileURLs.take(5)
      val arrayOfRowsURLsNoBraces = for (e <- arrayOfRowsURLs) yield e.toString().replaceAll("[\\[\\]]", "")
      val arrayOfRowsURLsFinal = for (e <- arrayOfRowsURLsNoBraces) yield e.toString() split (",")
      arrayOfRowsURLsFinal(0)(0).toString.substring(0, 5)

      val dataURLs = ObservableBuffer(Seq(
        (arrayOfRowsURLsFinal(0)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(0)(0).toString.length, 50)), arrayOfRowsURLsFinal(0)(1).toInt),
        (arrayOfRowsURLsFinal(1)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(1)(0).toString.length, 50)), arrayOfRowsURLsFinal(1)(1).toInt),
        (arrayOfRowsURLsFinal(2)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(2)(0).toString.length, 50)), arrayOfRowsURLsFinal(2)(1).toInt),
        (arrayOfRowsURLsFinal(3)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(3)(0).toString.length, 50)), arrayOfRowsURLsFinal(3)(1).toInt),
        (arrayOfRowsURLsFinal(4)(0).toString.substring(0, Math.min(arrayOfRowsURLsFinal(4)(0).toString.length, 50)), arrayOfRowsURLsFinal(4)(1).toInt)

      ) map { case (x, y) => XYChart.Data[String, Number](x, y) })


      val seriesURLs = XYChart.Series[String, Number]("URLs posted by the people with the most followers", dataURLs)
      barChartHashtagsURLsCount.getData.add(seriesURLs)
      barChartHashtagsURLsCount.visible = true
      val file2 = new File("Output/Followers/")
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "
      barChartHashtagsURLsCount.setTitle("Top 5 URLs by Followers Of The Account")

      return
    }
    if (actionValue.equals("Sentiments Piechart(English Only)")) {
      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/Sentiment/**/*csv")

      val arrayOfRowsURLs: Array[Row] = dataFromFileURLs.take(3)
      val arrayOfRowsURLsNoBraces = for (e <- arrayOfRowsURLs) yield e.toString().replaceAll("[\\[\\]]", "")
      val arrayOfRowsURLsFinal = for (e <- arrayOfRowsURLsNoBraces) yield e.toString() split (",")
      val percentagePositive = (arrayOfRowsURLsFinal(0)(1).toDouble/(arrayOfRowsURLsFinal(0)(1).toDouble+ arrayOfRowsURLsFinal(1)(1).toDouble+arrayOfRowsURLsFinal(2)(1).toDouble))*100
      val percentageNeutral=(arrayOfRowsURLsFinal(1)(1).toDouble/(arrayOfRowsURLsFinal(0)(1).toDouble+ arrayOfRowsURLsFinal(1)(1).toDouble+arrayOfRowsURLsFinal(2)(1).toDouble))*100
      val percentageNegative=(arrayOfRowsURLsFinal(2)(1).toDouble/(arrayOfRowsURLsFinal(0)(1).toDouble+ arrayOfRowsURLsFinal(1)(1).toDouble+arrayOfRowsURLsFinal(2)(1).toDouble))*100
      val data = ObservableBuffer(Seq((arrayOfRowsURLsFinal(2)(0).toString + "("+ percentageNegative.toString.substring(0,4)+"%)", arrayOfRowsURLsFinal(2)(1).toInt), (arrayOfRowsURLsFinal(1)(0).toString + "("+ percentageNeutral.toString.substring(0,4)+"%)", arrayOfRowsURLsFinal(1)(1).toInt), (arrayOfRowsURLsFinal(0)(0).toString+"("+ percentagePositive.toString.substring(0,4)+"%)", arrayOfRowsURLsFinal(0)(1).toInt)))map {case (x, y) => PieChart.Data(x, y)}

      pieChart.getData.addAll(data)


      pieChart.visible = true
      val file2 = new File("Output/Sentiment/")
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "







      return
    }
    if (actionValue.equals("Sentiment Bubblechart(English Only)")) {





            val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/SentimentBubbleChart/**/*csv")
            dataFromFileURLs.createOrReplaceTempView("fromFile")

            val filteredDFNegative =  ss.sql("SELECT followers, time, statuses, sentiment FROM fromFile where sentiment<0")
     // val filteredDFNeutral =  ss.sql("SELECT followers, time, statuses, sentiment FROM fromFile where sentiment=0")
      val filteredDFPositive =  ss.sql("SELECT followers, time, statuses, sentiment FROM fromFile where sentiment>0")
      val filteredDFAll =  ss.sql("SELECT followers, time, statuses, sentiment FROM fromFile")


      import scala.collection.JavaConverters._

      val itNegative: Iterator[Row] = filteredDFNegative.toLocalIterator().asScala.toIterable.iterator
     // val itNeutral: Iterator[Row] = filteredDFNeutral.toLocalIterator().asScala.toIterable.iterator
      val itPositive: Iterator[Row] = filteredDFPositive.toLocalIterator().asScala.toIterable.iterator
      val itAll: Iterator[Row] = filteredDFAll.toLocalIterator().asScala.toIterable.iterator




      val toChartData = (t: Row) => XYChart.Data[Number, Number](t.get(1).asInstanceOf[String].toInt*100000, t.get(2).asInstanceOf[String].toInt, t.get(0).asInstanceOf[String].toInt/2)


      val negativeData = itNegative.toSeq.map(toChartData)
      //val neutralData = itNeutral.toSeq.map(toChartData)
      val positiveData = itPositive.toSeq.map(toChartData)



      val seriesNegative = new XYChart.Series[Number, Number] {
        name = "Negative"
        data = negativeData
      }
      val seriesNeutral = new XYChart.Series[Number, Number] {
        name = "Neutral"
        data = Seq()
      }
      val seriesPositive = new XYChart.Series[Number, Number] {
        name = "Positive"
        data = positiveData

      }
      import scala.math.random

      def randomData = (1 to 20).map(
        _ => XYChart.Data[Number, Number](random * 100, random * 100, random * 10))
      val bcSeries1 = XYChart.Series("Neutral", ObservableBuffer(randomData))
      xaxis.setLowerBound(100000)
      xaxis.setUpperBound(60*100000)
      xaxis.setTickUnit(100000)

      bubbleChart.getData.add(seriesNegative)
      bubbleChart.getData.add(bcSeries1)
      bubbleChart.getData.add(seriesPositive)
      bubbleChart.visible = true
      val file2 = new File("Output/SentimentBubbleChart/")
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "

      return
    }


    if (actionValue.equals("Language Piechart Tweet Count")) {
      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/LanguagePieChart/**/*csv")


      import scala.collection.JavaConverters._


      val iterator = dataFromFileURLs.toLocalIterator().asScala.toIterator


      while(iterator.hasNext){

        val next = iterator.next
        languagePieChart.getData.add(PieChart.Data(next.get(0).toString,next.get(1).asInstanceOf[String].toInt ))

      }



      languagePieChart.visible = true
      val file2 = new File("Output/LanguagePieChart/")
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "
      languagePieChart.setTitle("Language Piechart Tweet Count")

      return
    }
    if (actionValue.equals("Language Piechart Influence(Followers)")) {
      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/LanguageFollowersPieChart/**/*csv")


      import scala.collection.JavaConverters._


      val iterator = dataFromFileURLs.toLocalIterator().asScala.toIterator


      while(iterator.hasNext){

        val next = iterator.next
        languagePieChart.getData.add(PieChart.Data(next.get(0).toString,next.get(1).asInstanceOf[String].toInt ))

      }



      languagePieChart.visible = true
      val file2 = new File("Output/LanguageFollowersPieChart/")
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "
      languagePieChart.setTitle("Influence Of Language Piechart")
      return
    }
    if (actionValue.equals("Timeline of Sentiments of Tweets")) {



      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/LineChartSentimentFollowers/**/*csv")
      dataFromFileURLs.createOrReplaceTempView("fromFile")

      val filteredDFNegative =  ss.sql("SELECT cast(followers as double) followers, time, sentiment FROM fromFile where sentiment='Negative'")
      val filteredDFNeutral =  ss.sql("SELECT  cast(followers as double) followers, time, sentiment FROM fromFile where sentiment='Neutral'")
      val filteredDFPositive =  ss.sql("SELECT  cast(followers as double) followers, time, sentiment FROM fromFile where sentiment='Positive'")
      val filteredDFAll =  ss.sql("SELECT followers, time, sentiment FROM fromFile")
      filteredDFNegative.createOrReplaceTempView("Negative")
      filteredDFNeutral.createOrReplaceTempView("Neutral")
      filteredDFPositive.createOrReplaceTempView("Positive")
      val finalDFNegative =  ss.sql("SELECT max(followers), time FROM Negative group by time")
      val finalDFNeutral =  ss.sql("SELECT max(followers), time FROM Neutral group by time")
      val finalDFPositive =  ss.sql("SELECT max(followers), time FROM Positive group by time")

      import scala.collection.JavaConverters._

      val itNegative: Iterator[Row] = finalDFNegative.toLocalIterator().asScala.toIterable.iterator
       val itNeutral: Iterator[Row] = finalDFNeutral.toLocalIterator().asScala.toIterable.iterator
      val itPositive: Iterator[Row] = finalDFPositive.toLocalIterator().asScala.toIterable.iterator
      val itAll: Iterator[Row] = filteredDFAll.toLocalIterator().asScala.toIterable.iterator
      filteredDFNeutral.show(10)


      val toChartData = (t: Row) => XYChart.Data[Number, Number](t.get(1).asInstanceOf[String].toInt, t.get(0).toString.toDouble)


      val negativeData = itNegative.toSeq.map(toChartData)
      val neutralData = itNeutral.toSeq.map(toChartData)
      val positiveData = itPositive.toSeq.map(toChartData)



      val seriesNegative = new XYChart.Series[Number, Number] {
        name = "Negative"
        data = negativeData
      }
      val seriesNeutral = new XYChart.Series[Number, Number] {
        name = "Neutral"
        data = neutralData
      }
      val seriesPositive = new XYChart.Series[Number, Number] {
        name = "Positive"
        data = positiveData

      }

      lineChart.getData.add(seriesNegative)
      lineChart.getData.add(seriesNeutral)
      lineChart.getData.add(seriesPositive)
      lineChart.visible = true
      val file2 = new File("Output/LineChartSentimentFollowers/")
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "
      lineChart.setTitle("Timeline of Sentiments of Tweets vs Followers")

      return
    }
    if (actionValue.equals("Timeline of Languages of Tweets")) {
      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/timelineOfLanguages/**/*csv")
      dataFromFileURLs.createOrReplaceTempView("fromFile")

      val arrayList = new util.ArrayList[DataFrame]

      val languages = ss.sql("SELECT distinct(lang) FROM fromFile")
      val iterator1 = languages.toLocalIterator()
      while(iterator1.hasNext){
        val next = iterator1.next()
        val view = ss.sql("SELECT cast(followers as double) followers, time, lang FROM fromFile where lang='"+next.get(0).toString+"'")
        view.createOrReplaceTempView("Temp")
        val finalDFNegative =  ss.sql("SELECT max(followers), time, first(lang) FROM Temp group by time")

        arrayList.add(finalDFNegative)

      }
      val toChartData = (t: Row) => XYChart.Data[Number, Number](t.get(1).asInstanceOf[String].toInt, t.get(0).toString.toDouble)

      import scala.collection.JavaConverters._

      val iterator3 = arrayList.asScala.iterator

      while(iterator3.hasNext){
        val nextIterator = iterator3.next.toLocalIterator().asScala.toIterator
        while(nextIterator.hasNext){

          val next: Row = nextIterator.next()
          val series = new XYChart.Series[Number, Number] {
            name = next.get(2).toString
            data = nextIterator.toSeq.map(toChartData)

          }
          lineChart.getData.add(series)

        }


      }



      lineChart.visible = true

      val file2 = new File("Output/timelineOfLanguages/")
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)
      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "
      lineChart.setTitle("Timeline of Languages of Tweets vs Followers")

      return
    }
  }

  def onClose(event: ActionEvent) {
    System.exit(0)
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

