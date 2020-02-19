//https://github.com/scalafx/ProScalaFX/blob/master/src/proscalafx/ch07/ChartApp8.scala
//how to draw a graph


import java.io.File
import java.util.Locale.Category

import scalafx.event.ActionEvent
import scalafx.scene.control.{Button, ChoiceBox, TextArea, TextField}
import scalafx.scene.layout.GridPane
import scalafxml.core.macros.sfxml
import analyzer.{TwitterAnalyzer, _}
import com.lynden.gmapsfx.GoogleMapView
import org.apache.commons.io.FileUtils
import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.spark_project.guava.collect.Iterables
import scalafx.scene.chart.{BarChart, CategoryAxis, LineChart, NumberAxis, XYChart}
import scalafx.collections.ObservableBuffer
import org.apache.commons.io.FileUtils.cleanDirectory


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
                                       private val buttonSubmit: Button) {

  def handleSubmit(event: ActionEvent): Unit = {
    val tweetStream = new Array[String](8)
    tweetStream(0) = consumerKeyField.text.value
    tweetStream(1) = consumerSecretField.text.value
    tweetStream(2) = accessSecretField.text.value
    tweetStream(3) = accessTokenField.text.value
    tweetStream(4) = languagesField.text.value
    tweetStream(5) = wordsField.text.value
    tweetStream(6) = numberOfTweetsField.text.value
    tweetStream(7) = actionChoiceBox.getValue()
    additionalInfoTextArea.text = "Job has been completed. Please look in the terminal for its output and in folder output for its . Please press \"Graph\" to see the graph about the job. Please note, that after you hit graph you will have to resubmit the job to obtain the files with full result as they will be deleted. The same will happen if you press clear."
    TwitterAnalyzer.main(tweetStream)
    actionChoiceBox.setDisable(true)
    buttonGraph.setDisable(false)
    buttonSubmit.setDisable(true)
    barChartHashtagsURLsCount.getData.clear()
  }

  def handleClear(event: ActionEvent): Unit = {
    consumerKeyField.text = ""
    consumerSecretField.text = ""
    accessSecretField.text = ""
    accessTokenField.text = ""
    consumerSecretField.text = ""
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

    if (actionValue.equals("Word Count Hashtags")) {
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
    if (actionValue.equals("Word Count URLs")) {

      val dataFromFileURLs = ss.read.format("csv").option("header", "true").load("Output/URLs/**/*csv")

      val arrayOfRowsURLs = dataFromFileURLs.take(5)
      val arrayOfRowsURLsNoBraces = for (e <- arrayOfRowsURLs) yield e.toString().replaceAll("[\\[\\]]", "")
      val arrayOfRowsURLsFinal = for (e <- arrayOfRowsURLsNoBraces) yield e.toString() split (",")
      arrayOfRowsURLsFinal(0)(0).toString.substring(0, 5)

      val dataURLs = ObservableBuffer(Seq(
        (arrayOfRowsURLsFinal(0)(0).toString, arrayOfRowsURLsFinal(0)(1).toInt),
        (arrayOfRowsURLsFinal(1)(0).toString, arrayOfRowsURLsFinal(1)(1).toInt),
        (arrayOfRowsURLsFinal(2)(0).toString, arrayOfRowsURLsFinal(2)(1).toInt),
        (arrayOfRowsURLsFinal(3)(0).toString, arrayOfRowsURLsFinal(3)(1).toInt),
        (arrayOfRowsURLsFinal(4)(0).toString, arrayOfRowsURLsFinal(4)(1).toInt)

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
        (arrayOfRowsURLsFinal(0)(0).toString, arrayOfRowsURLsFinal(0)(1).toInt),
        (arrayOfRowsURLsFinal(1)(0).toString, arrayOfRowsURLsFinal(1)(1).toInt),
        (arrayOfRowsURLsFinal(2)(0).toString, arrayOfRowsURLsFinal(2)(1).toInt),
        (arrayOfRowsURLsFinal(3)(0).toString, arrayOfRowsURLsFinal(3)(1).toInt),
        (arrayOfRowsURLsFinal(4)(0).toString, arrayOfRowsURLsFinal(4)(1).toInt)

      ) map { case (x, y) => XYChart.Data[String, Number](x, y) })

      val seriesURLs = XYChart.Series[String, Number]("URLs", dataURLs)
      barChartHashtagsURLsCount.getData.addAll(seriesHashTags, seriesURLs)
      barChartHashtagsURLsCount.visible = true

      additionalInfoTextArea.text = "Graph Job has been completed. Please look in the terminal for its output. "

      val file1 = new File("Output/Hashtags/")
      val file2 = new File("Output/URLs/")

      FileUtils.cleanDirectory(file1);
      FileUtils.cleanDirectory(file2);
      buttonGraph.setDisable(true)
      actionChoiceBox.setDisable(false)
      buttonSubmit.setDisable(false)

    }
  }

  def onClose(event: ActionEvent) {
    System.exit(0)
  }

}

