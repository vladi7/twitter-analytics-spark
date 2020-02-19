Twitter Analytics Project

This goal of this project is to enable any user to analyze tweets. This is the first phase of this project and the goal for the phase to count the hashtags and URLs. The counted hashtags and urls can be found under OutputWithCounts directory in their respective folders. The spark output as well as hadoop outputs can be found in OutputLogs folder

Also, as that can be seen from the GUIScreenshot, the group was able to complete the task that will be useful in the future phases. The group used SparkSQL to store tweets and extract hashtags and urls as well, and then use ScalaFX visualization tools to graph the top 5 Hashtags and URLs in the bar chart.

This project was tested in Intellij with Java 8.0.242.fx-librca obtained with sdkman. It also uses a python script written for Python 2.7 to pull the tweets. For tweet analysis we used Scala 2.12.10, Hadoop 3.2.1(to store tweets), Spark 2.4.4, Ubuntu 18.04(dualboot).


The ultimate goal for this project is to provide the user with the ability to analyze up to 10 queries about trends of twitter, with the topic that is chosen by the user.
That can help monitor elections, awards, brands, traffic situations, health situations, disaster situations, stocks, etc. Analyzing this data can provide crucial information about various topics which can help in the serious situations that need swift response like disasters or stocks.

A secondary goal would be to analyze tweets sentiment. That might be one of the queries that we ran with Spark SQL since the twitter JSON already has the sentiment field. However, another goal would be to use machine learning model and compare that twitter analysis to machine learning analysis that is done ourselves.



The python script will try to store the data pulled from twitter on hdfs local host:9000 and will fail otherwise.

The GUI component works as follows:
1) User has to input their keys for twitter, the words to look for in the tweets(choose topic), languages that should be used,what count to run(or both of them), and number of tweets that the user wants to pull. 
The program was tested on 100000, however, we believe it can handle more. The App is not responding should be ignored since in the terminal we can see that the program is still pulling the tweets, analyzing it.
2) After tweets were pulled ana hashtags/urls were counted, the counts will be saved in the Output folder in their respective folders.
Please note that the Graph button is enabled and we can graph top 5 HashTags/URLs.
After we graph the data we pulled, the files with counts will be deleted and the program will be ready to pull and analyze more data. The benefit of this approach as we can do analysis one after another one. The downfall of it is we have to remove the counts output, however, for submission purposes I saved it in another folder called OutputWithCounts.
The same will happen if we press Clear.