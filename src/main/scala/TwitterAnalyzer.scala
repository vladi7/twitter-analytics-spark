
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

import com.google.gson.Gson
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter._
import twitter4j.TwitterFactory
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import sys.process._

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    def callPython(): Unit = {
      val result = "python TweetStreamer.py" ! ProcessLogger(stdout append _, stderr append _)
      println(result)
      println("stdout: " + stdout)
      println("stderr: " + stderr)
    }
    callPython()
  }

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}