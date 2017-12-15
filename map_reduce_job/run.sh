hdfs dfs -rm -r /input
hdfs dfs -rm -r /dataDividedByUser
hdfs dfs -rm -r /coOccurrenceMatrix
hdfs dfs -rm -r /normalize
hdfs dfs -rm -r /averageRating
hdfs dfs -rm -r /movieList
hdfs dfs -rm -r /ratingMatrix
hdfs dfs -rm -r /multiplication
hdfs dfs -rm -r /summation

hdfs dfs -mkdir /input
hdfs dfs -put ../dataset/* /input

hadoop com.sun.tools.javac.Main *.java

jar cf recommender.jar *.class

hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /normalize /averageRating /movieList /ratingMatrix /multiplication /summation
