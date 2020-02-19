name := "TwitterAnalyticsProject"

version := "0.1"

scalaVersion := "2.12.10"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xcheckinit", "-encoding", "utf8")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-mllib"% "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-streaming" % "2.4.4",
  "org.twitter4j" % "twitter4j-core" % "3.0.3",
  "org.twitter4j" % "twitter4j-stream" % "3.0.3",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
  "io.spray" %%  "spray-json" % "1.3.4",
  "com.typesafe.play" % "play-json_2.11"% "2.4.4",
  "org.scalafx" %% "scalafx" % "8.0.192-R14",
  "org.scalafx" %% "scalafxml-core-sfx8" % "0.5",
  "com.lynden" % "GMapsFX" % "1.1.1"


)
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
unmanagedJars in Compile += Attributed.blank(file("~/.sdkman/candidates/java/8.0.242.fx-librca/jre/lib/jfxswt.jar"))